package registry

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/writeaheadlog"
)

// newTestWal is a helper method to create a WAL for testing.
func newTestWAL(path string) *writeaheadlog.WAL {
	_, wal, err := writeaheadlog.New(path)
	if err != nil {
		panic(err)
	}
	return wal
}

func testDir(name string) string {
	dir := build.TempDir(name)
	_ = os.RemoveAll(dir)
	err := os.MkdirAll(dir, modules.DefaultDirPerm)
	if err != nil {
		panic(err)
	}
	return dir
}

// TestNew is a unit test for New. It confirms that New can initialize an empty
// registry and load existing items from disk.
func TestNew(t *testing.T) {
	dir := testDir(t.Name())
	wal := newTestWAL(filepath.Join(dir, "wal"))

	// Create a new registry.
	registryPath := filepath.Join(dir, "registry")
	r, err := New(registryPath, wal)
	if err != nil {
		t.Fatal(err)
	}

	// The first call should simply init it. Check the size and version.
	expected := make([]byte, persistedEntrySize)
	binary.LittleEndian.PutUint64(expected, registryVersion)
	b, err := ioutil.ReadFile(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, expected) {
		t.Fatal("metadata doesn't match")
	}

	// The entries map should be empty.
	if len(r.entries) != 0 {
		t.Fatal("registry shouldn't contain any entries")
	}

	// Save a random unused entry at the first index and a used entry at the
	// second index.
	vUnused := randomValue(1)
	vUsed := randomValue(2)
	err = r.saveEntry(vUnused, false)
	if err != nil {
		t.Fatal(err)
	}
	err = r.saveEntry(vUsed, true)
	if err != nil {
		t.Fatal(err)
	}

	// Load the registry again. 'New' should load the used entry from disk but
	// not the unused one.
	r, err = New(registryPath, wal)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.entries) != 1 {
		t.Fatal("registry should contain one entry", len(r.entries))
	}
	if v, exists := r.entries[vUsed.mapKey()]; !exists || !reflect.DeepEqual(*v, vUsed) {
		t.Log(v)
		t.Log(vUsed)
		t.Fatal("registry contains wrong key-value pair")
	}
}

// TestUpdate is a unit test for Update. It makes sure new entries are added
// correctly, old ones are updated and that unused slots on disk are filled.
func TestUpdate(t *testing.T) {
	dir := testDir(t.Name())
	wal := newTestWAL(filepath.Join(dir, "wal"))

	// Create a new registry.
	registryPath := filepath.Join(dir, "registry")
	r, err := New(registryPath, wal)
	if err != nil {
		t.Fatal(err)
	}

	// Register a value.
	v := randomValue(2)
	v.staticIndex = 1 // expected index
	updated, err := r.Update(v.key, v.tweak, v.expiry, v.revision, v.data)
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("key shouldn't have existed before")
	}
	if len(r.entries) != 1 {
		t.Fatal("registry should contain one entry", len(r.entries))
	}
	if vExist, exists := r.entries[v.mapKey()]; !exists || !reflect.DeepEqual(*vExist, v) {
		t.Log(v)
		t.Log(*vExist)
		t.Fatal("registry contains wrong key-value pair")
	}

	// Update the same key again. This shouldn't work cause the revision is the
	// same.
	_, err = r.Update(v.key, v.tweak, v.expiry, v.revision, v.data)
	if !errors.Contains(err, errInvalidRevNum) {
		t.Fatal("expected invalid rev number")
	}

	// Try again with a higher revision number. This should work.
	v.revision++
	updated, err = r.Update(v.key, v.tweak, v.expiry, v.revision, v.data)
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("key should have existed before")
	}
	if len(r.entries) != 1 {
		t.Fatal("registry should contain one entry", len(r.entries))
	}
	if vExist, exists := r.entries[v.mapKey()]; !exists || !reflect.DeepEqual(*vExist, v) {
		t.Log(v)
		t.Log(*vExist)
		t.Fatal("registry contains wrong key-value pair")
	}

	// Try another update with too much data.
	v.revision++
	_, err = r.Update(v.key, v.tweak, v.expiry, v.revision, make([]byte, RegistryDataSize+1))
	if !errors.Contains(err, errTooMuchData) {
		t.Fatal("expected too much data")
	}

	// Add a second entry.
	v2 := randomValue(2)
	v2.staticIndex = 2 // expected index
	updated, err = r.Update(v2.key, v2.tweak, v2.expiry, v2.revision, v2.data)
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("key shouldn't have existed before")
	}
	if len(r.entries) != 2 {
		t.Fatal("registry should contain two entries", len(r.entries))
	}
	if vExist, exists := r.entries[v2.mapKey()]; !exists || !reflect.DeepEqual(*vExist, v2) {
		t.Log(v2)
		t.Log(*vExist)
		t.Fatal("registry contains wrong key-value pair")
	}

	// Mark the first entry as unused and save it to disk.
	err = r.saveEntry(v, false)
	if err != nil {
		t.Fatal(err)
	}

	// Reload the registry Only the second entry should exist.
	r, err = New(registryPath, wal)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.entries) != 1 {
		t.Fatal("registry should contain one entries", len(r.entries))
	}
	if vExist, exists := r.entries[v2.mapKey()]; !exists || !reflect.DeepEqual(*vExist, v2) {
		t.Log(v2)
		t.Log(*vExist)
		t.Fatal("registry contains wrong key-value pair")
	}

	// Update the registry with a third entry. It should get the index that the
	// first entry had before.
	v3 := randomValue(2)
	v3.staticIndex = v.staticIndex // expected index
	updated, err = r.Update(v3.key, v3.tweak, v3.expiry, v3.revision, v3.data)
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("key shouldn't have existed before")
	}
	if len(r.entries) != 2 {
		t.Fatal("registry should contain two entries", len(r.entries))
	}
	if vExist, exists := r.entries[v3.mapKey()]; !exists || !reflect.DeepEqual(*vExist, v3) {
		t.Log(v3)
		t.Log(*vExist)
		t.Fatal("registry contains wrong key-value pair")
	}
}

// TestPrune is a unit test for Prune.
func TestPrune(t *testing.T) {
	dir := testDir(t.Name())
	wal := newTestWAL(filepath.Join(dir, "wal"))

	// Create a new registry.
	registryPath := filepath.Join(dir, "registry")
	r, err := New(registryPath, wal)
	if err != nil {
		t.Fatal(err)
	}

	// Add 2 entries with different expiries.
	v1 := randomValue(1)
	v1.expiry = 1
	_, err = r.Update(v1.key, v1.tweak, v1.expiry, v1.revision, v1.data)
	if err != nil {
		t.Fatal(err)
	}
	v2 := randomValue(2)
	v2.expiry = 2
	_, err = r.Update(v2.key, v2.tweak, v2.expiry, v2.revision, v2.data)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 2 entries.
	if len(r.entries) != 2 {
		t.Fatal("wrong number of entries")
	}

	// Purge 1 of them.
	err = r.Prune(1)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 1 entry.
	if len(r.entries) != 1 {
		t.Fatal("wrong number of entries")
	}
	if vExist, exists := r.entries[v2.mapKey()]; !exists || !reflect.DeepEqual(*vExist, v2) {
		t.Log(v2)
		t.Log(*vExist)
		t.Fatal("registry contains wrong key-value pair")
	}

	// Restart.
	_, err = New(registryPath, wal)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 1 entry.
	if len(r.entries) != 1 {
		t.Fatal("wrong number of entries")
	}
	if vExist, exists := r.entries[v2.mapKey()]; !exists || !reflect.DeepEqual(*vExist, v2) {
		t.Log(v2)
		t.Log(*vExist)
		t.Fatal("registry contains wrong key-value pair")
	}
}
