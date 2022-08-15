package skyd

import (
	"gitlab.com/SkynetLabs/skyd/node/api"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/crypto"
	"testing"
)

// TestCacheBase covers the base functionality of PinnedSkylinksCache:
// * NewCache
// * Add
// * Contains
// * Diff
// * Remove
func TestCacheBase(t *testing.T) {
	t.Parallel()

	sl1 := "A_CuSb3BpGxmSbRAg1xj5T8SdB4hiSFiEW2sEEzxt5MNkg"
	sl2 := "B_CuSb3BpGxmSbRAg1xj5T8SdB4hiSFiEW2sEEzxt5MNkg"
	sl3 := "C_CuSb3BpGxmSbRAg1xj5T8SdB4hiSFiEW2sEEzxt5MNkg"

	c := NewCache()
	if c.Contains(sl1) {
		t.Fatal("Should not contain ", sl1)
	}
	c.Add(sl1)
	if !c.Contains(sl1) {
		t.Fatal("Should contain ", sl1)
	}
	c.Remove(sl1)
	if c.Contains(sl1) {
		t.Fatal("Should not contain ", sl1)
	}

	// Add sl1 and sl2 to the cache.
	c.Add(sl1)
	c.Add(sl2)
	// Diff a list of sl2 and sl3 against the cache.
	// Expect to get sl1 as missing and sl3 as unknown.
	u, m := c.Diff([]string{sl2, sl3})
	if len(m) != 1 || m[0] != sl1 {
		t.Fatalf("Expected to get '%s' as the single 'missing' result but got %v", sl3, m)
	}
	if len(u) != 1 || u[0] != sl3 {
		t.Fatalf("Expected to get '%s' as the single 'unknown' result but got %v", sl1, u)
	}
}

// TestCacheRebuild covers the Rebuild functionality of PinnedSkylinksCache.
func TestCacheRebuild(t *testing.T) {
	t.Parallel()

	sl := "XX_uSb3BpGxmSbRAg1xj5T8SdB4hiSFiEW2sEEzxt5MNkg"

	// This skylink exists on the mock filesystem.
	slBlocked := "CAClyosjvI9Fg75N-LRylcfba79bam9Ljp-4qfxS08Q__B"
	var slbl skymodules.Skylink
	err := slbl.LoadString(slBlocked)
	if err != nil {
		t.Fatal(err)
	}
	// Add the blocked skylink to a blocklist.
	blocklist := api.SkynetBlocklistGET{
		Blocklist: []crypto.Hash{slbl.MerkleRoot()},
	}

	c := NewCache()
	// Add a skylink to the cache. Expect this to be gone after the rebuild.
	c.Add(sl)
	skyd := NewSkydClientMock()
	skyd.SetBlocklist(blocklist)
	sls := skyd.MockFilesystem()
	rr := c.Rebuild(skyd)
	// Wait for the rebuild to finish.
	<-rr.ErrAvail
	if rr.ExternErr != nil {
		t.Fatal(rr.ExternErr)
	}
	// Ensure that the blocked skylink is not in the cache.
	if c.Contains(slBlocked) {
		t.Fatalf("Expected blocked skylink '%s' to not be present after the rebuild.", sl)
	}
	// Ensure that all expected skylinks are in the cache now.
	for _, s := range sls {
		if s != slBlocked && !c.Contains(s) {
			t.Fatalf("Expected skylink '%s' to be in the cache.", s)
		}
	}
	// Ensure that the skylink we added before the rebuild is gone.
	if c.Contains(sl) {
		t.Fatalf("Expected skylink '%s' to not be present after the rebuild.", sl)
	}
}
