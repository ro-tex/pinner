package database

import (
	"testing"

	"github.com/skynetlabs/pinner/database"
	"github.com/skynetlabs/pinner/test"
	"gitlab.com/NebulousLabs/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

// TestSkylink is a comprehensive test suite that covers the base functionality
// of the Skylink database type.
//
// Tested methods:
// * CreateSkylink
// * FindSkylink
// * MarkPinned
// * MarkUnpinned
// * AddServerForSkylinks
// * RemoveServerFromSkylinks
func TestSkylink(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	cfg, err := test.LoadTestConfig()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := test.Context()
	defer cancel()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}

	sl1 := test.RandomSkylink()
	sl2 := test.RandomSkylink()

	// Fetch the skylinks from the DB. Expect ErrSkylinkNotExist.
	_, err = db.FindSkylink(ctx, sl1)
	if !errors.Contains(err, database.ErrSkylinkNotExist) {
		t.Fatalf("Expected error %v, got %v.", database.ErrSkylinkNotExist, err)
	}
	_, err = db.FindSkylink(ctx, sl2)
	if !errors.Contains(err, database.ErrSkylinkNotExist) {
		t.Fatalf("Expected error %v, got %v.", database.ErrSkylinkNotExist, err)
	}
	// Create the skylinks.
	s1, err := db.CreateSkylink(ctx, sl1, cfg.ServerName)
	if err != nil {
		t.Fatal("failed to create a skylink:", err)
	}
	s2, err := db.CreateSkylink(ctx, sl2, cfg.ServerName)
	if err != nil {
		t.Fatal("failed to create a skylink:", err)
	}
	// Validate that the underlying skylink is the same.
	if s1.Skylink != sl1.String() {
		t.Fatalf("Expected skylink '%s', got '%s'", sl1, s1.Skylink)
	}
	if s2.Skylink != sl2.String() {
		t.Fatalf("Expected skylink '%s', got '%s'", sl2, s2.Skylink)
	}
	// Add the skylink again, expect this to fail with ErrSkylinkExists.
	otherServer := "second create"
	_, err = db.CreateSkylink(ctx, sl1, otherServer)
	if !errors.Contains(err, database.ErrSkylinkExists) {
		t.Fatalf("Expected '%v', got '%v'", database.ErrSkylinkExists, err)
	}
	// Clean up.
	err = db.RemoveServerFromSkylinks(ctx, []string{sl1.String(), sl2.String()}, otherServer)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure that they're gone.
	s1, err = db.FindSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if test.Contains(s1.Servers, otherServer) {
		t.Fatalf("Did not expect to have %s there", otherServer)
	}
	s2, err = db.FindSkylink(ctx, sl2)
	if err != nil {
		t.Fatal(err)
	}
	if test.Contains(s2.Servers, otherServer) {
		t.Fatalf("Did not expect to have %s there", otherServer)
	}
	// Mark sl1 as failed once.
	err = db.MarkFailedAttempt(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}

	// Add a new server to the list.
	server := "new server"
	err = db.AddServerForSkylinks(ctx, []string{sl1.String(), sl2.String()}, server, false)
	if err != nil {
		t.Fatal(err)
	}
	s1, err = db.FindSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	s2, err = db.FindSkylink(ctx, sl2)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the new server was added to the list.
	if !test.Contains(s1.Servers, server) {
		t.Fatalf("Expected to find '%s' in the list, got '%v'", server, s1.Servers)
	}
	if !test.Contains(s2.Servers, server) {
		t.Fatalf("Expected to find '%s' in the list, got '%v'", server, s2.Servers)
	}
	// Make sure sl1 is no longer marked as failed.
	s1, err = db.FindSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if s1.FailedAttempts > 0 {
		t.Fatalf("Expected zero failed attempts, got %d", s1.FailedAttempts)
	}
	// Remove a server from the list.
	err = db.RemoveServerFromSkylinks(ctx, []string{sl1.String(), sl2.String()}, server)
	if err != nil {
		t.Fatal(err)
	}
	s1, err = db.FindSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the new server was added to the list.
	if len(s1.Servers) != 1 || s1.Servers[0] != cfg.ServerName {
		t.Fatalf("Expected to find only '%s' in the list, got '%v'", cfg.ServerName, s1.Servers)
	}
	// Mark the file as unpinned.
	err = db.MarkUnpinned(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	s1, err = db.FindSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if s1.Pinned {
		t.Fatal("Expected the skylink to be unpinned.")
	}
	// Mark the file as pinned again.
	err = db.MarkPinned(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	s1, err = db.FindSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if !s1.Pinned {
		t.Fatal("Expected the skylink to be pinned.")
	}
	// Mark the skylink as unpinned again.
	err = db.MarkUnpinned(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	// Add a server to it with the `markUnpinned` set to false.
	// Expect the skylink to remain unpinned.
	err = db.AddServerForSkylinks(ctx, []string{sl1.String()}, "new server pin false", false)
	if err != nil {
		t.Fatal(err)
	}
	s1, err = db.FindSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if s1.Pinned {
		t.Fatal("Expected the skylink to be unpinned.")
	}
	// Mark sl1 as failed once.
	err = db.MarkFailedAttempt(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	// Add a server to the skylink with `markUnpinned` set to true.
	// Expect the skylink to be pinned.
	err = db.AddServerForSkylinks(ctx, []string{sl1.String()}, "new server pin true", true)
	if err != nil {
		t.Fatal(err)
	}
	s1, err = db.FindSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if !s1.Pinned {
		t.Fatal("Expected the skylink to be pinned.")
	}
	// Make sure sl1 is no longer marked as failed.
	if s1.FailedAttempts > 0 {
		t.Fatalf("Expected zero failed attempts, got %d", s1.FailedAttempts)
	}
	// Delete the skylink.
	err = db.DeleteSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	s1, err = db.FindSkylink(ctx, sl1)
	if !errors.Contains(err, database.ErrSkylinkNotExist) {
		t.Fatalf("Expected error %v, got %v.", database.ErrSkylinkNotExist, err)
	}

	// Add a server for non-existent skylinks.
	sl3 := test.RandomSkylink()
	sl4 := test.RandomSkylink()
	err = db.AddServerForSkylinks(ctx, []string{sl3.String(), sl4.String()}, "new server pin true", true)
	if err != nil {
		t.Fatal(err)
	}
	s3, err := db.FindSkylink(ctx, sl3)
	if err != nil {
		t.Fatal(err)
	}
	if s3.Skylink != sl3.String() {
		t.Fatal("Mismatch")
	}
	s4, err := db.FindSkylink(ctx, sl4)
	if err != nil {
		t.Fatal(err)
	}
	if s4.Skylink != sl4.String() {
		t.Fatal("Mismatch")
	}
}

// TestFindAndLock tests the functionality of FindAndLockUnderpinned and
// UnlockSkylink.
func TestFindAndLock(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	cfg, err := test.LoadTestConfig()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := test.Context()
	defer cancel()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}

	sl := test.RandomSkylink()

	// We start with a minimum number of pinners set to 1.
	cfg.MinPinners = 1

	// Try to fetch an underpinned skylink, expect none to be found.
	_, err = db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{}, cfg.MinPinners)
	if !errors.Contains(err, database.ErrNoUnderpinnedSkylinks) {
		t.Fatalf("Expected to get '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}
	// Create a new skylink.
	_, err = db.CreateSkylink(ctx, sl, cfg.ServerName)
	if err != nil {
		t.Fatal(err)
	}
	// Try to fetch an underpinned skylink, expect none to be found.
	_, err = db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{}, cfg.MinPinners)
	if !errors.Contains(err, database.ErrNoUnderpinnedSkylinks) {
		t.Fatalf("Expected to get '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}
	// Make sure it's pinned by fewer than the minimum number of servers.
	err = db.RemoveServerFromSkylinks(ctx, []string{sl.String()}, cfg.ServerName)
	if err != nil {
		t.Fatal(err)
	}
	// Try to fetch underpinned skylinks but skip sl. Expect none to be found.
	_, err = db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{sl.String()}, cfg.MinPinners)
	if !errors.Contains(err, database.ErrNoUnderpinnedSkylinks) {
		t.Fatalf("Expected to get '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}
	// Try to fetch an underpinned skylink, expect to find one.
	underpinned, err := db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{}, cfg.MinPinners)
	if err != nil {
		t.Fatal(err)
	}
	if !underpinned.Equals(sl) {
		t.Fatalf("Expected to get '%s', got '%v'", sl, underpinned)
	}
	// Try to fetch an underpinned skylink from the name of a different server.
	// Expect to find none because the one we got before is now locked and
	// shouldn't be returned.
	_, err = db.FindAndLockUnderpinned(ctx, "different server", []string{}, cfg.MinPinners)
	if !errors.Contains(err, database.ErrNoUnderpinnedSkylinks) {
		t.Fatalf("Expected to get '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}
	// Add a pinner.
	err = db.AddServerForSkylinks(ctx, []string{sl.String()}, cfg.ServerName, false)
	if err != nil {
		t.Fatal(err)
	}
	err = db.UnlockSkylink(ctx, sl, cfg.ServerName)
	if err != nil {
		t.Fatal(err)
	}
	// Try to fetch an underpinned skylink, expect none to be found.
	_, err = db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{}, cfg.MinPinners)
	if !errors.Contains(err, database.ErrNoUnderpinnedSkylinks) {
		t.Fatalf("Expected to get '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}

	// Increase the minimum number of pinners to two.
	cfg.MinPinners = 2

	anotherServerName := "another server"
	thirdServerName := "third server"

	// Try to fetch an underpinned skylink, expect none to be found.
	// Out test skylink is underpinned but it's pinned by the given server, so
	// we expect it not to be returned.
	_, err = db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{}, cfg.MinPinners)
	if !errors.Contains(err, database.ErrNoUnderpinnedSkylinks) {
		t.Fatalf("Expected to get '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}
	// Try to fetch an underpinned skylink from the name of a different server.
	// Expect one to be found.
	_, err = db.FindAndLockUnderpinned(ctx, anotherServerName, []string{}, cfg.MinPinners)
	if err != nil {
		t.Fatal(err)
	}
	// Add a pinner.
	err = db.AddServerForSkylinks(ctx, []string{sl.String()}, anotherServerName, false)
	if err != nil {
		t.Fatal(err)
	}
	// Try to unlock the skylink from the name of a server that hasn't locked
	// it. Expect this to fail.
	err = db.UnlockSkylink(ctx, sl, thirdServerName)
	if !errors.Contains(err, database.ErrNoSkylinksLocked) {
		t.Fatalf("Expected to get '%v', got '%v'", database.ErrNoSkylinksLocked, err)
	}
	err = db.UnlockSkylink(ctx, sl, anotherServerName)
	if err != nil {
		t.Fatal(err)
	}
	// Try to fetch an underpinned skylink with a third server name, expect none
	// to be found because our skylink is now properly pinned.
	_, err = db.FindAndLockUnderpinned(ctx, thirdServerName, []string{}, cfg.MinPinners)
	if !errors.Contains(err, database.ErrNoUnderpinnedSkylinks) {
		t.Fatalf("Expected to get '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}
}

// TestFindAndLock ensures that FindAndLockUnderpinned will first check
// for files currently locked by the current server and only after that it will
// lock new ones.
func TestFindAndLockOwnFirst(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	cfg, err := test.LoadTestConfig()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := test.Context()
	defer cancel()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Create two skylinks from the name of another server and set the minimum
	// number of pinners to two, so these show up as underpinned.
	sl1 := test.RandomSkylink()
	sl2 := test.RandomSkylink()
	cfg.MinPinners = 2
	otherServer := "other server"
	_, err = db.CreateSkylink(ctx, sl1, otherServer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.CreateSkylink(ctx, sl2, otherServer)
	if err != nil {
		t.Fatal(err)
	}
	// Fetch and lock one of those.
	locked, err := db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{}, cfg.MinPinners)
	if err != nil {
		t.Fatal(err)
	}
	// Add this server to the list of pinners, so we're sure that it's not being
	// randomly selected. This will ensure that the same skylink is being
	// returned because it's locked by the current server.
	err = db.AddServerForSkylinks(ctx, []string{locked.String()}, cfg.ServerName, false)
	if err != nil {
		t.Fatal(err)
	}
	// Try fetching another underpinned skylink before unlocking this one.
	// Expect to get a different one.
	newLocked, err := db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{}, cfg.MinPinners)
	if err != nil {
		t.Fatal(err)
	}
	if newLocked == locked {
		t.Fatal("Expected to get a different skylink.")
	}
	// Unlock it.
	err = db.UnlockSkylink(ctx, locked, cfg.ServerName)
	if err != nil {
		t.Fatal(err)
	}
	// Fetch a new underpinned skylink. Expect it to fail because we've run out
	// of underpinned skylinks.
	newLocked, err = db.FindAndLockUnderpinned(ctx, cfg.ServerName, []string{}, cfg.MinPinners)
	if !errors.Contains(err, database.ErrNoUnderpinnedSkylinks) {
		t.Fatalf("Expected '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}
}

// TestServersForSkylink ensures that ServersForSkylink works as expected.
func TestServersForSkylink(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	ctx, cancel := test.Context()
	defer cancel()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}

	sl1 := test.RandomSkylink()

	srv1 := "server1"
	srv2 := "server2"

	// List all servers pinning sl1. Expect an empty list.
	ls, err := db.ServersForSkylink(ctx, sl1)
	if !errors.Contains(err, database.ErrSkylinkNotExist) {
		t.Fatalf("Expected '%v', got '%v'", database.ErrSkylinkNotExist, err)
	}
	if len(ls) > 0 {
		t.Fatal("Expected an empty list, got", ls)
	}
	// Add one server.
	err = db.AddServerForSkylinks(ctx, []string{sl1.String()}, srv1, true)
	if err != nil {
		t.Fatal(err)
	}
	// We expect to get that server now.
	ls, err = db.ServersForSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 1 || ls[0] != srv1 {
		t.Fatalf("Expected a list with one element - '%s', got '%v'", srv1, ls)
	}
	// Add another server.
	err = db.AddServerForSkylinks(ctx, []string{sl1.String()}, srv2, true)
	if err != nil {
		t.Fatal(err)
	}
	// We expect to get both servers.
	ls, err = db.ServersForSkylink(ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 2 || !test.Contains(ls, srv1) || !test.Contains(ls, srv2) {
		t.Fatalf("Expected a list with two elements - '%s' and '%s', got '%v'", srv1, srv2, ls)
	}
}

// TestSkylinksForServer ensures that SkylinksForServer works as expected.
func TestSkylinksForServer(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	ctx, cancel := test.Context()
	defer cancel()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}

	sl1 := test.RandomSkylink()
	sl2 := test.RandomSkylink()

	srv1 := "server1"
	srv2 := "server2"

	// List all skylinks pinned by svr1. Expect an empty list.
	_, err = db.SkylinksForServer(ctx, srv1)
	if !errors.Contains(err, mongo.ErrNoDocuments) {
		t.Fatal(err)
	}
	// Add a skylink.
	_, err = db.CreateSkylink(ctx, sl1, srv1)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure it shows up on the list.
	ls, err := db.SkylinksForServer(ctx, srv1)
	if err != nil {
		t.Fatal(err)
	}
	if !test.Contains(ls, sl1.String()) {
		t.Fatalf("Expected a list containing only %s but got %+v", sl1.String(), ls)
	}
	// Add another skylink.
	_, err = db.CreateSkylink(ctx, sl2, srv1)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure it shows up on the list.
	ls, err = db.SkylinksForServer(ctx, srv1)
	if err != nil {
		t.Fatal(err)
	}
	if !test.Contains(ls, sl1.String()) || !test.Contains(ls, sl2.String()) {
		t.Fatalf("Expected a list containing both %s and %s but got %+v", sl1.String(), sl2.String(), ls)
	}
	// Add a second server to sl1 and expect it to still show up on the list.
	err = db.AddServerForSkylinks(ctx, []string{sl1.String()}, srv2, false)
	if err != nil {
		t.Fatal(err)
	}
	ls, err = db.SkylinksForServer(ctx, srv1)
	if err != nil {
		t.Fatal(err)
	}
	if !test.Contains(ls, sl1.String()) {
		t.Fatalf("Expected a list containing both %s and %s but got %+v", sl1.String(), sl2.String(), ls)
	}
	// Remove srv1 as pinner for both sl1 and sl2.
	err = db.RemoveServerFromSkylinks(ctx, []string{sl1.String(), sl2.String()}, srv1)
	if err != nil {
		t.Fatal(err)
	}
	// Expect an empty list for srv1.
	_, err = db.SkylinksForServer(ctx, srv1)
	if !errors.Contains(err, mongo.ErrNoDocuments) {
		t.Fatal(err)
	}
	// Expect sl1 to still appear in the list of srv2.
	ls, err = db.SkylinksForServer(ctx, srv2)
	if err != nil {
		t.Fatal(err)
	}
	if !test.Contains(ls, sl1.String()) {
		t.Fatalf("Expected a list containing only %s but got %+v", sl1.String(), ls)
	}
}

// TestFailedAttempts ensures that MarkFailedAttempt and ResetFailedAttempts
// function as expected.
func TestFailedAttempts(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	ctx, cancel := test.Context()
	defer cancel()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}

	sl := test.RandomSkylink()
	srv := "server"

	_, err = db.CreateSkylink(ctx, sl, srv)
	if err != nil {
		t.Fatal(err)
	}
	err = db.MarkFailedAttempt(ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	err = db.MarkFailedAttempt(ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	sl1, err := db.FindSkylink(ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	if sl1.FailedAttempts != 2 {
		t.Fatalf("Expected %d failed attempts, got %d", 2, sl1.FailedAttempts)
	}
	err = db.ResetFailedAttempts(ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	sl2, err := db.FindSkylink(ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	if sl2.FailedAttempts != 0 {
		t.Fatalf("Expected %d failed attempts, got %d", 0, sl2.FailedAttempts)
	}
}
