// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionCompact(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		// setup fresh backend
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
		require.Len(t, kvs, 7)

		// test partial compaction - should remove files with revision <= 3
		// this should remove: revision 1,2,3 (a/b/c, a, b)
		_, err = b.Compact(ctx, 3)
		require.NoError(t, err)

		// verify remaining keys after compaction
		_, kvs, err = b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 4)

		// test full compaction - all keys should be removed
		_, err = b.Compact(ctx, 7)
		require.NoError(t, err)

		_, kvs, err = b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Empty(t, kvs)
	})

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

	t.Run("compact_empty_directories", func(t *testing.T) {
		baseDir, err := filepath.Abs(t.TempDir())
		if err != nil {
			t.Fatalf("DB path error: %v", err)
		}

		// setup fresh backend
		b := setupBackend(t, baseDir)
		ctx := context.Background()

		// create deeply nested keys
		_, err = b.Create(ctx, "a/b/c/d/e/f", []byte("deep"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "a/b/c/d/e/g", []byte("deep"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "a/b/c/h", []byte("mid"), 0)
		require.NoError(t, err)

		// list to see all keys
		rev, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 3)

		// verify directories exist before compaction
		require.DirExists(t, filepath.Join(baseDir, "a"))
		require.DirExists(t, filepath.Join(baseDir, "a/b"))
		require.DirExists(t, filepath.Join(baseDir, "a/b/c"))
		require.DirExists(t, filepath.Join(baseDir, "a/b/c/d"))
		require.DirExists(t, filepath.Join(baseDir, "a/b/c/d/e"))
		require.DirExists(t, filepath.Join(baseDir, "a/b/c/d/e/f"))
		require.DirExists(t, filepath.Join(baseDir, "a/b/c/d/e/g"))
		require.DirExists(t, filepath.Join(baseDir, "a/b/c/h"))

		// verify that data files exist before compaction
		fDir := filepath.Join(baseDir, "a/b/c/d/e/f")
		fEntries, err := os.ReadDir(fDir)
		require.NoError(t, err)
		require.NotEmpty(t, fEntries, "Expected at least one file in f directory")

		// compact everything
		_, err = b.Compact(ctx, rev)
		require.NoError(t, err)

		// verify no keys remain
		_, kvs, err = b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Empty(t, kvs)

		// verify all nested directories have been cleaned up
		require.NoDirExists(t, filepath.Join(baseDir, "a/b/c/d/e/f"))
		require.NoDirExists(t, filepath.Join(baseDir, "a/b/c/d/e/g"))
		require.NoDirExists(t, filepath.Join(baseDir, "a/b/c/d/e"))
		require.NoDirExists(t, filepath.Join(baseDir, "a/b/c/d"))
		require.NoDirExists(t, filepath.Join(baseDir, "a/b/c/h"))
		require.NoDirExists(t, filepath.Join(baseDir, "a/b/c"))
		require.NoDirExists(t, filepath.Join(baseDir, "a/b"))
		require.NoDirExists(t, filepath.Join(baseDir, "a"))

		// the base data path should still exist
		require.DirExists(t, baseDir)

		// verify base directory is empty
		entries, err := os.ReadDir(baseDir)
		require.NoError(t, err)
		// expect only the counter file to remain
		require.Len(t, entries, 1)
		require.Equal(t, "counter", entries[0].Name())
	})

	t.Run("compact_idempotent", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create some keys
		for i := range 5 {
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
		require.Empty(t, kvs)
	})

	t.Run("compact_with_options", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key with multiple revisions
		_, err := b.Create(ctx, "key", []byte("val1"), 0)
		require.NoError(t, err)

		rev2, _, ok, err := b.Update(ctx, "key", []byte("val2"), 1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		rev3, _, ok, err := b.Update(ctx, "key", []byte("val3"), rev2, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// verify we can get by specific revision
		_, kv, err := b.Get(ctx, "key", "", rev2, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("val2"), kv.Value)

		// compact to revision 4 (should remove revisions 1-3)
		_, err = b.Compact(ctx, rev3+1)
		require.NoError(t, err)

		// verify we can't get by compacted revision
		_, _, err = b.Get(ctx, "key", "", rev2, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "required revision has been compacted")

		// try to compact to an already compacted revision (should fail)
		_, err = b.Compact(ctx, rev2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "required revision has been compacted")
	})
}
