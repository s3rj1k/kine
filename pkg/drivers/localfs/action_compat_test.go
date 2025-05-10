// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/k3s-io/kine/pkg/drivers/localfs"
	"github.com/stretchr/testify/require"
)

func TestActionCompact(t *testing.T) {
	b := setupBackend(t)
	ctx := context.Background()

	// create several keys with different hierarchies
	// a/b/c -> nested keys
	// a     -> parent key
	// b     -> standalone key
	// a/b   -> intermediate key
	// c     -> another standalone
	// d/a   -> different hierarchy
	// d/b   -> different hierarchy
	_, err := b.Create(ctx, "a/b/c", nil, 0)
	require.NoError(t, err)
	_, err = b.Create(ctx, "a", nil, 0)
	require.NoError(t, err)
	_, err = b.Create(ctx, "b", nil, 0)
	require.NoError(t, err)
	_, err = b.Create(ctx, "a/b", nil, 0)
	require.NoError(t, err)
	_, err = b.Create(ctx, "c", nil, 0)
	require.NoError(t, err)
	_, err = b.Create(ctx, "d/a", nil, 0)
	require.NoError(t, err)
	_, err = b.Create(ctx, "d/b", nil, 0)
	require.NoError(t, err)

	// verify all 7 keys exist
	_, kvs, err := b.List(ctx, "", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, 7, len(kvs))

	// test partial compaction - should remove files with revision <= 3
	// this should remove: revision 1,2,3 (a/b/c, a, b)
	_, err = b.Compact(ctx, 3)
	require.NoError(t, err)

	// verify remaining keys after compaction
	_, kvs, err = b.List(ctx, "", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(kvs))

	// test full compaction - all keys should be removed
	_, err = b.Compact(ctx, 7)
	require.NoError(t, err)

	_, kvs, err = b.List(ctx, "", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(kvs))

	// test edge cases with subtests
	t.Run("compact_with_updates_and_deletes", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create initial key
		rev1, err := b.Create(ctx, "test/key1", []byte("v1"), 0)
		require.NoError(t, err)

		// update the key multiple times
		rev2, _, ok, err := b.Update(ctx, "test/key1", []byte("v2"), rev1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		rev3, _, ok, err := b.Update(ctx, "test/key1", []byte("v3"), rev2, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// verify all versions exist on disk before compaction
		_, kv, err := b.Get(ctx, "test/key1", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, rev3, kv.ModRevision)
		require.Equal(t, []byte("v3"), kv.Value)

		// compact up to rev2 - should remove v1 and v2
		compactRev, err := b.Compact(ctx, rev2)
		require.NoError(t, err)
		require.Equal(t, rev2, compactRev)

		// verify that latest version is still accessible
		_, kv, err = b.Get(ctx, "test/key1", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, rev3, kv.ModRevision)
		require.Equal(t, []byte("v3"), kv.Value)

		// delete the key
		rev4, deletedKv, deleted, err := b.Delete(ctx, "test/key1", 0)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, deletedKv)

		// compact including the delete
		compactRev, err = b.Compact(ctx, rev4)
		require.NoError(t, err)
		require.Equal(t, rev4, compactRev)

		// verify key no longer exists
		_, kv, err = b.Get(ctx, "test/key1", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)
	})

	// ToDo: fixme
	// t.Run("compact_with_expired_keys", func(t *testing.T) {
	// 	// setup fresh backend
	// 	b := setupBackend(t)
	// 	ctx := context.Background()

	// 	// create a key that expires immediately (lease=1)
	// 	_, err := b.Create(ctx, "expire/key1", []byte("exp1"), 1)
	// 	require.NoError(t, err)

	// 	// create a non-expiring key
	// 	rev2, err := b.Create(ctx, "normal/key1", []byte("normal1"), 0)
	// 	require.NoError(t, err)

	// 	// sleep to ensure expired key is past its lease
	// 	time.Sleep(2 * time.Second)

	// 	// create another key after expiration
	// 	rev3, err := b.Create(ctx, "expire/key1", []byte("new1"), 0)
	// 	require.NoError(t, err)

	// 	// compact with a revision between expired and new
	// 	compactRev, err := b.Compact(ctx, rev2)
	// 	require.NoError(t, err)
	// 	require.Equal(t, rev2, compactRev)

	// 	// verify expired version is gone but new version exists
	// 	_, kv, err := b.Get(ctx, "expire/key1", "", 0, 0)
	// 	require.NoError(t, err)
	// 	require.NotNil(t, kv)
	// 	require.Equal(t, []byte("new1"), kv.Value)
	// 	require.Equal(t, rev3, kv.ModRevision)

	// 	// verify normal key still exists
	// 	_, kv, err = b.Get(ctx, "normal/key1", "", 0, 0)
	// 	require.NoError(t, err)
	// 	require.NotNil(t, kv)
	// 	require.Equal(t, []byte("normal1"), kv.Value)
	// })

	t.Run("compact_empty_directories", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create deeply nested keys
		_, err := b.Create(ctx, "a/b/c/d/e/f", []byte("deep"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "a/b/c/d/e/g", []byte("deep"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "a/b/c/h", []byte("mid"), 0)
		require.NoError(t, err)

		// list to see all keys
		rev, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, 3, len(kvs))

		// cast backend to get DataBasePath for filesystem verification
		backend, ok := b.(*localfs.Backend)
		require.True(t, ok, "Failed to cast backend to *localfs.Backend")

		// verify directories exist before compaction
		require.DirExists(t, filepath.Join(backend.DataBasePath, "a"))
		require.DirExists(t, filepath.Join(backend.DataBasePath, "a/b"))
		require.DirExists(t, filepath.Join(backend.DataBasePath, "a/b/c"))
		require.DirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/d"))
		require.DirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/d/e"))
		require.DirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/d/e/f"))
		require.DirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/d/e/g"))
		require.DirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/h"))

		// verify that data files exist before compaction
		fDir := filepath.Join(backend.DataBasePath, "a/b/c/d/e/f")
		fEntries, err := os.ReadDir(fDir)
		require.NoError(t, err)
		require.Greater(t, len(fEntries), 0, "Expected at least one file in f directory")

		// compact everything
		_, err = b.Compact(ctx, rev)
		require.NoError(t, err)

		// verify no keys remain
		_, kvs, err = b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(kvs))

		// verify all nested directories have been cleaned up
		// the cleanupEmptyDirs function should remove all empty directories
		// starting from the deepest level and working up
		require.NoDirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/d/e/f"))
		require.NoDirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/d/e/g"))
		require.NoDirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/d/e"))
		require.NoDirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/d"))
		require.NoDirExists(t, filepath.Join(backend.DataBasePath, "a/b/c/h"))
		require.NoDirExists(t, filepath.Join(backend.DataBasePath, "a/b/c"))
		require.NoDirExists(t, filepath.Join(backend.DataBasePath, "a/b"))
		require.NoDirExists(t, filepath.Join(backend.DataBasePath, "a"))

		// the base data path should still exist
		require.DirExists(t, backend.DataBasePath)

		// verify base directory is empty
		entries, err := os.ReadDir(backend.DataBasePath)
		require.NoError(t, err)
		// expect only the counter file to remain
		require.Equal(t, 1, len(entries))
		require.Equal(t, "counter", entries[0].Name())
	})

	t.Run("compact_no_effect_on_future_revisions", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys
		rev1, err := b.Create(ctx, "key1", []byte("v1"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "key2", []byte("v2"), 0)
		require.NoError(t, err)
		rev3, err := b.Create(ctx, "key3", []byte("v3"), 0)
		require.NoError(t, err)

		// compact with revision in the past
		_, err = b.Compact(ctx, rev1)
		require.NoError(t, err)

		// all keys except the first should still exist
		_, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(kvs))

		// compact with revision beyond current - should compact all
		_, err = b.Compact(ctx, rev3+100)
		require.NoError(t, err)

		// all keys should be gone
		_, kvs, err = b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(kvs))
	})

	t.Run("compact_idempotent", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create some keys
		for i := 0; i < 5; i++ {
			_, err := b.Create(ctx, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("v%d", i)), 0)
			require.NoError(t, err)
		}

		// get current revision
		rev, _, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)

		// compact twice with same revision
		_, err = b.Compact(ctx, rev)
		require.NoError(t, err)

		_, err = b.Compact(ctx, rev)
		require.NoError(t, err)

		// verify no keys remain
		_, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(kvs))
	})
}
