package api

import (
	"context"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/skynetlabs/pinner/conf"
	"github.com/skynetlabs/pinner/database"
	"github.com/skynetlabs/pinner/lib"
	"github.com/skynetlabs/pinner/test"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"go.mongodb.org/mongo-driver/mongo"
)

// subtest defines the structure of a subtest
type subtest struct {
	name string
	test func(t *testing.T, tt *test.Tester)
}

// TestHandlers is a meta test that sets up a test instance of pinner and runs
// a suite of tests that ensure all handlers behave as expected.
func TestHandlers(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	tt, err := test.NewTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if errClose := tt.Close(); errClose != nil {
			t.Error(errors.AddContext(errClose, "failed to close tester"))
		}
	}()

	// Specify subtests to run
	tests := []subtest{
		{name: "Health", test: testHandlerHealthGET},
		{name: "ListServers", test: testHandlerListServersGET},
		{name: "ListSkylinks", test: testHandlerListSkylinksGET},
		{name: "Pin", test: testHandlerPinPOST},
		{name: "Unpin", test: testHandlerUnpinPOST},
		{name: "ServerRemove", test: testServerRemovePOST},
		{name: "Sweep", test: testHandlerSweep},
	}

	// Run subtests
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			tst.test(t, tt)
		})
	}
}

// testHandlerHealthGET tests the "GET /health" handler.
func testHandlerHealthGET(t *testing.T, tt *test.Tester) {
	status, _, err := tt.HealthGET()
	if err != nil {
		t.Fatal(err)
	}
	// DBAlive should never be false because if we couldn't reach the DB, we
	// wouldn't have made it this far in the test.
	if !status.DBAlive {
		t.Log(status)
		t.Fatal("DB down.")
	}
	if status.MinPinners != 1 {
		t.Fatalf("Expected min_pinners to have its default value of 1, got %d", status.MinPinners)
	}
	// Set a new min_pinners value.
	newMinPinners := 2
	err = tt.DB.SetConfigValue(tt.Ctx, conf.ConfMinPinners, strconv.Itoa(newMinPinners))
	if err != nil {
		t.Fatal(err)
	}
	// Verify the new value.
	status, _, err = tt.HealthGET()
	if err != nil {
		t.Fatal(err)
	}
	if status.MinPinners != newMinPinners {
		t.Fatalf("Expected %d, got %d", newMinPinners, status.MinPinners)
	}
}

// testHandlerListServersGET tests "GET /list/servers/:skylink"
func testHandlerListServersGET(t *testing.T, tt *test.Tester) {
	sl := test.RandomSkylink()
	srv1 := "server1"
	srv2 := "server2"
	// List servers for non-existent skylink.
	_, status, err := tt.ListServersGET(sl.String())
	if status != http.StatusNotFound || (err != nil && !strings.Contains(err.Error(), database.ErrSkylinkNotExist.Error())) {
		t.Fatalf("Expected %d '%v', got %d '%v'", http.StatusNotFound, database.ErrSkylinkNotExist, status, err)
	}
	// Add servers.
	_, err = tt.DB.CreateSkylink(tt.Ctx, sl, srv1)
	if err != nil {
		t.Fatal(err)
	}
	err = tt.DB.AddServerForSkylinks(tt.Ctx, []string{sl.String()}, srv2, true)
	if err != nil {
		t.Fatal(err)
	}
	servers, _, err := tt.ListServersGET(sl.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(servers) != 2 || !test.Contains(servers, srv1) || !test.Contains(servers, srv2) {
		t.Fatalf("Expected two servers, '%s' and '%s', got '%v'", srv1, srv2, servers)
	}
}

// testHandlerListSkylinksGET tests "GET /list/skylinks/:server"
func testHandlerListSkylinksGET(t *testing.T, tt *test.Tester) {
	sl1 := test.RandomSkylink()
	sl2 := test.RandomSkylink()
	server := hex.EncodeToString(fastrand.Bytes(16))
	// List skylinks for a non-existent server for non-existent skylink.
	_, status, err := tt.ListSkylinksGET(server)
	if status != http.StatusNotFound || (err != nil && !strings.Contains(err.Error(), mongo.ErrNoDocuments.Error())) {
		t.Fatalf("Expected %d '%v', got %d '%v'", http.StatusNotFound, mongo.ErrNoDocuments, status, err)
	}
	// Add skylinks.
	_, err = tt.DB.CreateSkylink(tt.Ctx, sl1, server)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tt.DB.CreateSkylink(tt.Ctx, sl2, server)
	if err != nil {
		t.Fatal(err)
	}
	skylinks, _, err := tt.ListSkylinksGET(server)
	if err != nil {
		t.Fatal(err)
	}
	if len(skylinks) != 2 || !test.Contains(skylinks, sl1.String()) || !test.Contains(skylinks, sl2.String()) {
		t.Fatalf("Expected two skylinks, '%s' and '%s', got '%v'", sl1.String(), sl2.String(), skylinks)
	}
}

// testHandlerPinPOST tests "POST /pin"
func testHandlerPinPOST(t *testing.T, tt *test.Tester) {
	sl := test.RandomSkylink()

	// Pin an invalid skylink.
	_, err := tt.PinPOST("this is not a skylink")
	if err == nil || !strings.Contains(err.Error(), database.ErrInvalidSkylink.Error()) {
		t.Fatalf("Expected error '%s', got '%v'", database.ErrInvalidSkylink, err)
	}
	// Pin a valid skylink.
	status, err := tt.PinPOST(sl.String())
	if err != nil || status != http.StatusNoContent {
		t.Fatal(status, err)
	}

	// Mark the skylink as unpinned and pin it again.
	// Expect it to no longer be unpinned.
	err = tt.DB.MarkUnpinned(tt.Ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	status, err = tt.PinPOST(sl.String())
	if err != nil || status != http.StatusNoContent {
		t.Fatal(status, err)
	}
	slNew, err := tt.DB.FindSkylink(tt.Ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	if !slNew.Pinned {
		t.Fatal("Expected the skylink to be pinned.")
	}
}

// testHandlerUnpinPOST tests "POST /unpin"
func testHandlerUnpinPOST(t *testing.T, tt *test.Tester) {
	sl := test.RandomSkylink()

	// Unpin an invalid skylink.
	_, err := tt.UnpinPOST("this is not a skylink")
	if err == nil || !strings.Contains(err.Error(), database.ErrInvalidSkylink.Error()) {
		t.Fatalf("Expected error '%s', got '%v'", database.ErrInvalidSkylink, err)
	}
	// Pin a valid skylink.
	status, err := tt.PinPOST(sl.String())
	if err != nil || status != http.StatusNoContent {
		t.Fatal(status, err)
	}
	// Unpin the skylink.
	status, err = tt.UnpinPOST(sl.String())
	if err != nil || status != http.StatusNoContent {
		t.Fatal(status, err)
	}
	// Make sure the skylink is marked as unpinned.
	slNew, err := tt.DB.FindSkylink(tt.Ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	if slNew.Pinned {
		t.Fatal("Expected the skylink to be marked as unpinned.")
	}
	// Unpin a valid skylink that's not in the DB, yet.
	sl2 := test.RandomSkylink()
	status, err = tt.UnpinPOST(sl2.String())
	if err != nil || status != http.StatusNoContent {
		t.Fatal(status, err)
	}
	// Make sure the skylink is marked as unpinned.
	sl2New, err := tt.DB.FindSkylink(tt.Ctx, sl)
	if err != nil {
		t.Fatal(err)
	}
	if sl2New.Pinned {
		t.Fatal("Expected the skylink to be marked as unpinned.")
	}
}

// testServerRemovePOST tests "POST /server/remove"
func testServerRemovePOST(t *testing.T, tt *test.Tester) {
	sl1 := test.RandomSkylink()
	sl2 := test.RandomSkylink()
	server := t.Name()

	// Pass empty server name.
	_, _, err := tt.ServerRemovePOST("")
	if err == nil || !strings.Contains(err.Error(), "no server found in request body") {
		t.Fatalf("Expected '%s', got '%s'", "no server found in request body", err)
	}
	// Remove a non-existent server. Expect no error, zero skylinks.
	r, status, err := tt.ServerRemovePOST(server)
	if err != nil || status != http.StatusOK || r.NumSkylinks != 0 {
		t.Fatalf("Expected no error, status 200, and zero skylinks affected, got error '%v', status %d and %d skylinks afffected", err, status, r.NumSkylinks)
	}
	// Create skylinks and mark them as pinned by the server.
	_, err = tt.DB.CreateSkylink(tt.Ctx, sl1, server)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tt.DB.CreateSkylink(tt.Ctx, sl2, server)
	if err != nil {
		t.Fatal(err)
	}
	// Set the next scan to be in 24 hours before removing the server.
	t0 := lib.Now().Add(24 * time.Hour)
	err = conf.SetNextScan(tt.Ctx, tt.DB, t0)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure we set it right.
	t1, err := conf.NextScan(tt.Ctx, tt.DB, tt.Logger)
	if err != nil {
		t.Fatal(err)
	}
	if t0 != t1 {
		t.Fatalf("Expected '%s', got '%s'", t0.String(), t1.String())
	}
	// Remove the server.
	r, status, err = tt.ServerRemovePOST(server)
	if err != nil || status != http.StatusOK {
		t.Fatal(status, err)
	}
	// Make sure there's a scan scheduled for about a DefaultNextScanOffset
	// later. Do this check ASAP after the removal or at least get the time
	// target.
	now := lib.Now()
	timeTarget := now.Add(conf.DefaultNextScanOffset)
	t2, err := conf.NextScan(tt.Ctx, tt.DB, tt.Logger)
	if err != nil {
		t.Fatal(err)
	}
	// This tolerance needs to be on the larger side because Mongo can be slow
	// on a system under load and we don't want NDFs. Given that we set the next
	// scan to be in 24h, we can afford to leave a lot of leeway here.
	tolerance := 5 * time.Second
	low := timeTarget.Add(-1 * tolerance)
	high := timeTarget.Add(tolerance)
	if t2.Before(low) || t2.After(high) {
		t.Fatalf(
			"Expected the next scan to be in %s (%s), got %s (%s)",
			conf.DefaultNextScanOffset.String(), timeTarget.String(), t2.Sub(now).String(), t2.String())
	}
	// Make sure the server's load record is gone.
	_, err = tt.DB.ServerLoad(tt.Ctx, server)
	if !errors.Contains(err, database.ErrServerLoadNotFound) {
		t.Fatalf("Expected '%v', got '%v'", database.ErrServerLoadNotFound, err)
	}
	// Make sure the response mentions two skylinks.
	if r.NumSkylinks != 2 {
		t.Fatalf("Expected 2 skylinks affected, got %d", r.NumSkylinks)
	}
	// Make sure the server is no longer marked as pinner for those two skylinks.
	foundSl, err := tt.DB.FindSkylink(tt.Ctx, sl1)
	if err != nil {
		t.Fatal(err)
	}
	if foundSl.Skylink != sl1.String() {
		t.Fatal("Unexpected skylink.")
	}
	if test.Contains(foundSl.Servers, server) {
		t.Fatalf("Expected to not find '%s' in servers list, got '%v'", server, foundSl.Servers)
	}
	// Same for the second skylink.
	foundSl, err = tt.DB.FindSkylink(tt.Ctx, sl2)
	if err != nil {
		t.Fatal(err)
	}
	if foundSl.Skylink != sl2.String() {
		t.Fatal("Unexpected skylink.")
	}
	if test.Contains(foundSl.Servers, server) {
		t.Fatalf("Expected to not find '%s' in servers list, got '%v'", server, foundSl.Servers)
	}
}

// testHandlerSweep tests both "POST /sweep" and "GET /sweep/status"
func testHandlerSweep(t *testing.T, tt *test.Tester) {
	// Prepare for the test by setting the state of skyd's mock.
	//
	// We'll have 3 skylinks:
	// 1 and 2 are pinned by skyd
	// 2 and 3 are marked in the database as pinned by skyd
	// What we expect after the sweep is to have 1 and 2 marked as pinned in the
	// database and 3 - not.
	sl1 := test.RandomSkylink()
	sl2 := test.RandomSkylink()
	sl3 := test.RandomSkylink()
	_, e1 := tt.SkydClient.Pin(sl1.String())
	_, e2 := tt.SkydClient.Pin(sl2.String())
	_, e3 := tt.PinPOST(sl2.String())
	_, e4 := tt.PinPOST(sl3.String())
	if e := errors.Compose(e1, e2, e3, e4); e != nil {
		t.Fatal(e)
	}

	// Check status. Expect zero value, no error.
	sweepStatus, code, err := tt.SweepStatusGET()
	if err != nil || code != http.StatusOK {
		t.Fatalf("Unexpected status code or error: %d %+v", code, err)
	}
	if sweepStatus.InProgress || !sweepStatus.StartTime.Equal(time.Time{}) {
		t.Fatalf("Unexpected sweep detected: %+v", sweepStatus)
	}
	// Start a sweep. Expect to return immediately with a 202.
	sweepReqTime := lib.Now()
	sr, code, err := tt.SweepPOST()
	if err != nil || code != http.StatusAccepted {
		t.Fatalf("Unexpected status code or error: %d %+v", code, err)
	}
	if sr.Href != "/sweep/status" {
		t.Fatalf("Unexpected href: '%s'", sr.Href)
	}
	// Make sure that the call returned quickly, i.e. it didn't wait for the
	// sweep to end but rather returned immediately and let the sweep run in the
	// background. Rebuilding the cache alone takes 100ms.
	if lib.Now().Add(-50 * time.Millisecond).After(sweepReqTime) {
		t.Fatal("Call to status took too long.")
	}
	// Check status. Expect a sweep in progress.
	sweepStatus, code, err = tt.SweepStatusGET()
	if err != nil || code != http.StatusOK {
		t.Fatalf("Unexpected status code or error: %d %+v", code, err)
	}
	if !sweepStatus.InProgress {
		t.Fatal("Expected to detect a sweep")
	}
	// Start a sweep.
	_, code, err = tt.SweepPOST()
	if err != nil || code != http.StatusAccepted {
		t.Fatalf("Unexpected status code or error: %d %+v", code, err)
	}
	// Check status. Expect the sweep start time to be the same as before, i.e.
	// no new sweep has been kicked off.
	initialSweepStartTime := sweepStatus.StartTime
	sweepStatus, code, err = tt.SweepStatusGET()
	if err != nil || code != http.StatusOK {
		t.Fatalf("Unexpected status code or error: %d %+v", code, err)
	}
	if !sweepStatus.InProgress {
		t.Fatal("Expected to detect a sweep")
	}
	if !sweepStatus.StartTime.Equal(initialSweepStartTime) {
		t.Fatalf("Expected the start time of the current scan to match the start time of the first scan we kicked off. Expected %v, got %v", initialSweepStartTime, sweepStatus.StartTime)
	}
	// Wait for the sweep to finish.
	for sweepStatus.InProgress {
		time.Sleep(100 * time.Millisecond)
		sweepStatus, code, err = tt.SweepStatusGET()
		if err != nil || code != http.StatusOK {
			t.Fatalf("Unexpected status code or error: %d %+v", code, err)
		}
		if !sweepStatus.InProgress {
			break
		}
	}
	// Make sure we have the expected database state - skylinks 1 and 2 are
	// pinned and 3 is not.
	skylinks, err := tt.DB.SkylinksForServer(context.Background(), tt.ServerName)
	if err != nil {
		t.Fatal(err)
	}
	if !test.Contains(skylinks, sl1.String()) {
		t.Fatalf("Expected %v to contain %s", skylinks, sl1.String())
	}
	if !test.Contains(skylinks, sl2.String()) {
		t.Fatalf("Expected %v to contain %s", skylinks, sl2.String())
	}
	if test.Contains(skylinks, sl3.String()) {
		t.Fatalf("Expected %v NOT to contain %s", skylinks, sl3.String())
	}
}
