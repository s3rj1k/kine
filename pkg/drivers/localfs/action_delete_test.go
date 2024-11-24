// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActionDelete(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key with lease
		rev1, err := b.Create(ctx, "a", []byte("b"), 1)
		require.NoError(t, err)
		require.Equal(t, int64(1), rev1)

		// verify key exists
		_, kv, err := b.Get(ctx, "a", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, "a", kv.Key)
		require.Equal(t, []byte("b"), kv.Value)

		// delete with specific revision
		rev2, kv, deleted, err := b.Delete(ctx, "a", rev1)
		require.NoError(t, err)
		require.Equal(t, int64(2), rev2) // delete creates a new revision
		require.True(t, deleted)
		require.NotNil(t, kv)
		require.Equal(t, "a", kv.Key)
		require.Equal(t, []byte("b"), kv.Value)
		require.Equal(t, int64(1), kv.Lease)
		require.Equal(t, int64(2), kv.ModRevision)
		require.Equal(t, int64(1), kv.CreateRevision)

		// verify key no longer exists
		_, kv, err = b.Get(ctx, "a", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)
	})

	t.Run("delete_with_zero_revision", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key without lease
		rev1, err := b.Create(ctx, "test", []byte("value"), 0)
		require.NoError(t, err)

		// update the key
		_, _, ok, err := b.Update(ctx, "test", []byte("updated"), rev1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// delete with zero revision, should delete the latest
		rev3, kv, deleted, err := b.Delete(ctx, "test", 0)
		require.NoError(t, err)
		require.Equal(t, int64(3), rev3)
		require.True(t, deleted)
		require.NotNil(t, kv)
		require.Equal(t, []byte("updated"), kv.Value)
		require.Equal(t, int64(3), kv.ModRevision)
		require.Equal(t, int64(1), kv.CreateRevision)
	})

	t.Run("delete_non_existent_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// delete non-existent key
		rev, kv, deleted, err := b.Delete(ctx, "does-not-exist", 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), rev)
		require.False(t, deleted)
		require.Nil(t, kv)
	})

	t.Run("delete_with_wrong_revision", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		rev1, err := b.Create(ctx, "key1", []byte("value1"), 0)
		require.NoError(t, err)

		// create another key to increment revision
		rev2, err := b.Create(ctx, "key2", []byte("value2"), 0)
		require.NoError(t, err)
		require.Greater(t, rev2, rev1)

		// delete key1 with wrong revision
		rev3, kv, deleted, err := b.Delete(ctx, "key1", 999)
		require.NoError(t, err)
		require.Equal(t, int64(2), rev3)
		require.False(t, deleted)
		require.Nil(t, kv)

		// check for key1 existence
		_, kv, err = b.Get(ctx, "key1", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("value1"), kv.Value)
	})

	t.Run("delete_already_deleted_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create and delete a key
		rev1, err := b.Create(ctx, "temp", []byte("temporary"), 0)
		require.NoError(t, err)

		_, kv, deleted, err := b.Delete(ctx, "temp", rev1)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, kv)

		// delete the same key again
		rev3, kv, deleted, err := b.Delete(ctx, "temp", 0)
		require.NoError(t, err)
		require.Equal(t, int64(2), rev3)
		require.False(t, deleted)
		require.Nil(t, kv) // no kv returned for already deleted entry
	})

	t.Run("delete_expired_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key with short lease
		rev1, err := b.Create(ctx, "expiring", []byte("will-expire"), 1)
		require.NoError(t, err)

		// wait for expiration
		time.Sleep(2 * time.Second)

		// delete expired key
		rev2, kv, deleted, err := b.Delete(ctx, "expiring", rev1)
		require.NoError(t, err)
		require.Equal(t, int64(1), rev2)
		require.False(t, deleted)
		require.Nil(t, kv)
	})

	t.Run("delete_nested_keys", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create nested keys
		keys := []string{
			"a/b/c",
			"a/b/d",
			"a/e",
			"f",
		}

		revs := make([]int64, len(keys))

		for i, key := range keys {
			rev, err := b.Create(ctx, key, []byte(key), 0)
			require.NoError(t, err)

			revs[i] = rev
		}

		// delete nested key
		_, kv, deleted, err := b.Delete(ctx, "a/b/c", revs[0])
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, kv)
		require.Equal(t, []byte("a/b/c"), kv.Value)

		// verify deleted key no longer exists
		_, kv, err = b.Get(ctx, "a/b/c", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)

		// verify other keys still exist
		for i, key := range keys[1:] {
			_, kv, err := b.Get(ctx, key, "", 0, 0)
			require.NoError(t, err)
			require.NotNil(t, kv)
			require.Equal(t, []byte(key), kv.Value)
			require.Equal(t, revs[i+1], kv.CreateRevision)
		}
	})

	t.Run("delete_after_multiple_updates", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		rev1, err := b.Create(ctx, "multi", []byte("v1"), 0)
		require.NoError(t, err)

		// update multiple times
		rev2, _, ok, err := b.Update(ctx, "multi", []byte("v2"), rev1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		rev3, _, ok, err := b.Update(ctx, "multi", []byte("v3"), rev2, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// delete with an old revision
		rev4, kv, deleted, err := b.Delete(ctx, "multi", rev1)
		require.NoError(t, err)
		require.Equal(t, int64(3), rev4)
		require.False(t, deleted) // delete should fail due to revision mismatch
		require.Nil(t, kv)

		// key should still exist with latest value
		_, kv, err = b.Get(ctx, "multi", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("v3"), kv.Value)

		// delete with correct current revision
		rev5, kv, deleted, err := b.Delete(ctx, "multi", rev3)
		require.NoError(t, err)
		require.Equal(t, int64(4), rev5) // new revision after delete
		require.True(t, deleted)
		require.NotNil(t, kv)
		require.Equal(t, []byte("v3"), kv.Value)

		// verify key is now deleted
		_, kv, err = b.Get(ctx, "multi", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)
	})

	t.Run("delete_with_special_characters_in_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys with special characters
		specialKeys := []string{
			"key-with-dash",
			"key_with_underscore",
			"key.with.dots",
			"key:with:colons",
			"key=with=equals",
		}

		revisions := make([]int64, len(specialKeys))

		for i, key := range specialKeys {
			rev, err := b.Create(ctx, key, []byte(key), 0)
			require.NoError(t, err)

			revisions[i] = rev
		}

		// delete keys with special characters
		for i, key := range specialKeys {
			_, kv, deleted, err := b.Delete(ctx, key, revisions[i])
			require.NoError(t, err)
			require.True(t, deleted)
			require.NotNil(t, kv)
			require.Equal(t, []byte(key), kv.Value)
		}

		// verify all keys are deleted
		for _, key := range specialKeys {
			_, kv, err := b.Get(ctx, key, "", 0, 0)
			require.NoError(t, err)
			require.Nil(t, kv)
		}
	})

	t.Run("delete_with_various_options", func(t *testing.T) {
		// define test cases
		tests := []struct {
			name        string
			setup       []string
			deleteKey   string
			deleteRev   int64
			wantDeleted bool
			wantKeys    []string
		}{
			{
				name:        "delete_single_key",
				setup:       []string{"a", "b", "c", "c/abc", "d"},
				deleteKey:   "c",
				deleteRev:   0,
				wantDeleted: true,
				wantKeys:    []string{"a", "b", "c/abc", "d"},
			},
			{
				name:        "delete_nonexistent_key",
				setup:       []string{"a", "b", "c", "c/abc", "d"},
				deleteKey:   "e",
				deleteRev:   0,
				wantDeleted: false,
				wantKeys:    []string{"a", "b", "c", "c/abc", "d"},
			},
			{
				name:        "delete_with_specific_revision",
				setup:       []string{"a", "b", "c", "c/abc", "d"},
				deleteKey:   "b",
				deleteRev:   2, // Will be replaced with actual revision
				wantDeleted: true,
				wantKeys:    []string{"a", "c", "c/abc", "d"},
			},
			{
				name:        "delete_with_wrong_revision",
				setup:       []string{"a", "b", "c", "c/abc", "d"},
				deleteKey:   "b",
				deleteRev:   999, // Intentionally wrong revision
				wantDeleted: false,
				wantKeys:    []string{"a", "b", "c", "c/abc", "d"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// setup fresh backend for each test
				b := setupBackend(t)
				ctx := context.Background()

				// create all setup keys
				revs := make(map[string]int64)
				for _, key := range tt.setup {
					rev, err := b.Create(ctx, key, []byte("value-"+key), 0)
					require.NoError(t, err)
					revs[key] = rev
				}

				// if revision is specific, use actual revision
				deleteRev := tt.deleteRev
				if deleteRev == 2 && len(tt.setup) >= 2 {
					deleteRev = revs[tt.setup[1]] // Use actual revision of second key
				}

				// perform delete operation
				_, deletedKv, deleted, err := b.Delete(ctx, tt.deleteKey, deleteRev)
				require.NoError(t, err)
				require.Equal(t, tt.wantDeleted, deleted)

				// If deleted, verify returned KV
				if tt.wantDeleted {
					require.NotNil(t, deletedKv)
					require.Equal(t, tt.deleteKey, deletedKv.Key)
				}

				// verify remaining keys using List
				_, kvs, err := b.List(ctx, "", "", 0, 0)
				require.NoError(t, err)

				// extract key names from KVs
				keys := make([]string, 0, len(kvs))
				for _, kv := range kvs {
					keys = append(keys, kv.Key)
				}

				// check that remaining keys match expected
				require.ElementsMatch(t, tt.wantKeys, keys)
			})
		}
	})

	t.Run("delete_after_operations", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create and modify keys
		rev1, err := b.Create(ctx, "key1", []byte("value1"), 0)
		require.NoError(t, err)

		_, err = b.Create(ctx, "key2", []byte("value2"), 0)
		require.NoError(t, err)

		rev3, _, ok, err := b.Update(ctx, "key1", []byte("updated"), rev1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// delete with original revision should fail (key has been updated)
		_, _, deleted, err := b.Delete(ctx, "key1", rev1)
		require.NoError(t, err)
		require.False(t, deleted)

		// key should still exist
		_, kv, err := b.Get(ctx, "key1", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("updated"), kv.Value)

		// delete with current revision should succeed
		_, deletedKv, deleted, err := b.Delete(ctx, "key1", rev3)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, deletedKv)
		require.Equal(t, []byte("updated"), deletedKv.Value)

		// key should no longer exist
		_, kv, err = b.Get(ctx, "key1", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)

		// delete with any revision (0) should delete key2
		_, deletedKv, deleted, err = b.Delete(ctx, "key2", 0)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, deletedKv)
		require.Equal(t, []byte("value2"), deletedKv.Value)

		// verify all keys are gone
		_, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Empty(t, kvs)
	})

	t.Run("delete_with_nested_keys", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create a set of nested keys
		nestedKeys := []string{
			"a/b/c",
			"a/b/d",
			"a/e",
			"f",
		}

		revs := make([]int64, len(nestedKeys))

		// create all keys
		for i, key := range nestedKeys {
			rev, err := b.Create(ctx, key, []byte(key), 0)
			require.NoError(t, err)
			revs[i] = rev
		}

		// delete nested key
		_, deletedKv, deleted, err := b.Delete(ctx, "a/b/c", revs[0])
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, deletedKv)
		require.Equal(t, []byte("a/b/c"), deletedKv.Value)

		// verify deleted key no longer exists
		_, kv, err := b.Get(ctx, "a/b/c", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)

		// verify other keys still exist
		for i, key := range nestedKeys[1:] {
			_, kv, err := b.Get(ctx, key, "", 0, 0)
			require.NoError(t, err)
			require.NotNil(t, kv)
			require.Equal(t, []byte(key), kv.Value)
			require.Equal(t, revs[i+1], kv.CreateRevision)
		}

		// delete key with any revision (0)
		_, deletedKv, deleted, err = b.Delete(ctx, "a/e", 0)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, deletedKv)
		require.Equal(t, []byte("a/e"), deletedKv.Value)

		// verify it's deleted
		_, kv, err = b.Get(ctx, "a/e", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)
	})

	t.Run("delete_already_deleted_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create and delete a key
		rev1, err := b.Create(ctx, "temp", []byte("temporary"), 0)
		require.NoError(t, err)

		// delete it first time
		_, kv, deleted, err := b.Delete(ctx, "temp", rev1)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, kv)

		// try to delete the same key again
		_, kv, deleted, err = b.Delete(ctx, "temp", 0)
		require.NoError(t, err)
		require.False(t, deleted)
		require.Nil(t, kv)
	})

	t.Run("delete_after_recreate", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		rev1, err := b.Create(ctx, "recycled", []byte("original"), 0)
		require.NoError(t, err)

		// delete key
		rev2, deletedKv, deleted, err := b.Delete(ctx, "recycled", rev1)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, deletedKv)
		require.Equal(t, []byte("original"), deletedKv.Value)

		// recreate key with different value
		rev3, err := b.Create(ctx, "recycled", []byte("recreated"), 0)
		require.NoError(t, err)
		require.Greater(t, rev3, rev2)

		// delete the recreated key
		_, deletedKv, deleted, err = b.Delete(ctx, "recycled", rev3)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, deletedKv)
		require.Equal(t, []byte("recreated"), deletedKv.Value)

		// verify key is deleted
		_, kv, err := b.Get(ctx, "recycled", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)
	})
}
