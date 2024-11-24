// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActionList(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys with different hierarchies
		_, err := b.Create(ctx, "a/b/c", []byte("nested"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "a", []byte("parent"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "b", []byte("standalone"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "a/b", []byte("intermediate"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "c", []byte("another"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "d/a", []byte("different1"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "d/b", []byte("different2"), 0)
		require.NoError(t, err)

		// list all keys
		rev, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(7), rev)
		require.Len(t, kvs, 7)
		requireKeysSorted(t, kvs)

		// list keys with prefix
		rev, kvs, err = b.List(ctx, "a", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(7), rev)
		require.Len(t, kvs, 3)
		requireKeysSorted(t, kvs)
		requireKeys(t, kvs, "a", "a/b", "a/b/c")

		// list keys >= start key
		rev, kvs, err = b.List(ctx, "", "b", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(7), rev)
		require.Len(t, kvs, 4)
		requireKeysSorted(t, kvs)
		requireKeys(t, kvs, "b", "c", "d/a", "d/b")

		// list keys up to a revision
		rev, kvs, err = b.List(ctx, "", "", 0, 3)
		require.NoError(t, err)
		require.Equal(t, int64(7), rev)
		require.Len(t, kvs, 3)
		requireKeysSorted(t, kvs)
		requireKeys(t, kvs, "a", "a/b/c", "b")

		// list keys with a limit
		rev, kvs, err = b.List(ctx, "", "", 4, 0)
		require.NoError(t, err)
		require.Equal(t, int64(7), rev)
		require.Len(t, kvs, 4)
		requireKeysSorted(t, kvs)
		requireKeys(t, kvs, "a", "a/b", "a/b/c", "b")

		// list keys with a limit after some start key
		rev, kvs, err = b.List(ctx, "", "b", 2, 0)
		require.NoError(t, err)
		require.Equal(t, int64(7), rev)
		require.Len(t, kvs, 2)
		requireKeysSorted(t, kvs)
		requireKeys(t, kvs, "b", "c")
	})

	t.Run("list_with_prefix_filtering", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys with different prefixes
		_, err := b.Create(ctx, "users/alice", []byte("alice"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "users/bob", []byte("bob"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "users/admin/root", []byte("root"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "config/app", []byte("config"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "config/db", []byte("db"), 0)
		require.NoError(t, err)

		// list all keys
		_, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 5)

		// list keys with "users" prefix
		_, kvs, err = b.List(ctx, "users", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 3)
		requireKeys(t, kvs, "users/admin/root", "users/alice", "users/bob")

		// list keys with "config" prefix
		_, kvs, err = b.List(ctx, "config", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 2)
		requireKeys(t, kvs, "config/app", "config/db")

		// list keys with more specific prefix
		_, kvs, err = b.List(ctx, "users/admin", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 1)
		requireKeys(t, kvs, "users/admin/root")
	})

	t.Run("list_with_startkey", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys in lexicographic order
		_, err := b.Create(ctx, "key_a", []byte("a"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "key_b", []byte("b"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "key_c", []byte("c"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "key_d", []byte("d"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "key_e", []byte("e"), 0)
		require.NoError(t, err)

		// list all keys
		_, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 5)

		// list starting from key_c (inclusive)
		_, kvs, err = b.List(ctx, "", "key_c", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 3)
		requireKeys(t, kvs, "key_c", "key_d", "key_e")

		// list with both prefix and startKey
		_, kvs, err = b.List(ctx, "key", "key_b", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 4)
		requireKeys(t, kvs, "key_b", "key_c", "key_d", "key_e")

		// startKey that doesn't exist - should start from next available
		_, kvs, err = b.List(ctx, "", "key_bb", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 3)
		requireKeys(t, kvs, "key_c", "key_d", "key_e")
	})

	t.Run("list_with_revision", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys at different revisions
		rev1, err := b.Create(ctx, "r1", []byte("v1"), 0)
		require.NoError(t, err)
		rev2, err := b.Create(ctx, "r2", []byte("v2"), 0)
		require.NoError(t, err)
		rev3, err := b.Create(ctx, "r3", []byte("v3"), 0)
		require.NoError(t, err)

		// list at latest revision
		rev, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, rev3, rev)
		require.Len(t, kvs, 3)

		// list at specific revision
		rev, kvs, err = b.List(ctx, "", "", 0, rev2)
		require.NoError(t, err)
		require.Equal(t, rev3, rev) // current revision is still returned
		require.Len(t, kvs, 2)      // but list is as of rev2
		requireKeys(t, kvs, "r1", "r2")

		// list at specific revision
		rev, kvs, err = b.List(ctx, "", "", 0, rev1)
		require.NoError(t, err)
		require.Equal(t, rev3, rev)
		require.Len(t, kvs, 1)
		requireKeys(t, kvs, "r1")

		// update a key
		rev4, _, ok, err := b.Update(ctx, "r1", []byte("v1_updated"), rev1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// list all keys
		rev, kvs, err = b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, rev4, rev)
		require.Len(t, kvs, 3)
	})

	t.Run("list_with_expired_keys", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create a mix of expiring and non-expiring keys
		_, err := b.Create(ctx, "persistent1", []byte("p1"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "ephemeral1", []byte("e1"), 1) // expires in 1 second
		require.NoError(t, err)
		_, err = b.Create(ctx, "persistent2", []byte("p2"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "ephemeral2", []byte("e2"), 1) // expires in 1 second
		require.NoError(t, err)

		// list all keys before expiration
		_, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 4)

		// wait for keys to expire
		time.Sleep(2 * time.Second)

		// list all keys after expiration, expired keys should not appear
		_, kvs, err = b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 2)
		requireKeys(t, kvs, "persistent1", "persistent2")
	})

	t.Run("list_after_deletes", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create several keys
		rev1, err := b.Create(ctx, "del1", []byte("d1"), 0)
		require.NoError(t, err)
		rev2, err := b.Create(ctx, "del2", []byte("d2"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "del3", []byte("d3"), 0)
		require.NoError(t, err)
		_, err = b.Create(ctx, "keep1", []byte("k1"), 0)
		require.NoError(t, err)

		// list all keys
		_, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 4)

		// delete some keys
		_, _, deleted, err := b.Delete(ctx, "del1", rev1)
		require.NoError(t, err)
		require.True(t, deleted)

		rev6, _, deleted, err := b.Delete(ctx, "del2", rev2)
		require.NoError(t, err)
		require.True(t, deleted)

		// list all keys
		rev, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, rev6, rev)
		require.Len(t, kvs, 2)
		requireKeys(t, kvs, "del3", "keep1")

		// list at revision before deletes
		rev, kvs, err = b.List(ctx, "", "", 0, 3)
		require.NoError(t, err)
		require.Equal(t, rev6, rev)
		require.Len(t, kvs, 1)
		requireKeys(t, kvs, "del3")
	})

	t.Run("list_with_limit", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create many keys
		for i := range 10 {
			key := string(rune('a' + i))
			_, err := b.Create(ctx, key, []byte(key), 0)
			require.NoError(t, err)
		}

		// list with different limits
		limits := []int64{1, 3, 5, 10, 20}
		for _, limit := range limits {
			_, kvs, err := b.List(ctx, "", "", limit, 0)
			require.NoError(t, err)

			expectedCount := int(limit)
			if expectedCount > 10 {
				expectedCount = 10
			}

			require.Len(t, kvs, expectedCount)
			requireKeysSorted(t, kvs)
		}
	})

	t.Run("list_with_complex_filters", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys with complex hierarchy
		keys := []string{
			"app/v1/config",
			"app/v1/data",
			"app/v2/config",
			"app/v2/data",
			"app/v2/cache",
			"system/logs",
			"system/metrics",
		}

		for i, key := range keys {
			_, err := b.Create(ctx, key, []byte(string(rune('a'+i))), 0)
			require.NoError(t, err)
		}

		// list with prefix and limit
		_, kvs, err := b.List(ctx, "app/v2", "", 2, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 2)
		require.Contains(t, []string{"app/v2/cache", "app/v2/config"}, kvs[0].Key)
		require.Contains(t, []string{"app/v2/cache", "app/v2/config"}, kvs[1].Key)

		// list with prefix, startKey and limit
		_, kvs, err = b.List(ctx, "app", "app/v2", 2, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 2)
		requireKeysSorted(t, kvs)
		require.GreaterOrEqual(t, kvs[0].Key, "app/v2")
	})

	t.Run("list_with_compaction", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys across multiple revisions
		rev1, err := b.Create(ctx, "compact1", []byte("c1"), 0)
		require.NoError(t, err)
		rev2, err := b.Create(ctx, "compact2", []byte("c2"), 0)
		require.NoError(t, err)
		rev3, err := b.Create(ctx, "compact3", []byte("c3"), 0)
		require.NoError(t, err)

		// list all keys
		_, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Len(t, kvs, 3)

		// compact up to rev2
		_, err = b.Compact(ctx, rev2)
		require.NoError(t, err)

		// list all keys
		rev, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, rev3, rev)
		require.Len(t, kvs, 1)
		requireKeys(t, kvs, "compact3")

		// list at revision before compaction (should still work)
		rev, kvs, err = b.List(ctx, "", "", 0, rev1)
		require.NoError(t, err)
		require.Equal(t, rev3, rev)
		require.Empty(t, kvs) // compacted data not available
	})

	t.Run("list_empty_database", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// list empty database
		rev, kvs, err := b.List(ctx, "", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), rev)
		require.Empty(t, kvs)

		// list with various parameters on empty db
		_, kvs, err = b.List(ctx, "prefix", "", 0, 0)
		require.NoError(t, err)
		require.Empty(t, kvs)

		_, kvs, err = b.List(ctx, "", "startkey", 0, 0)
		require.NoError(t, err)
		require.Empty(t, kvs)

		_, kvs, err = b.List(ctx, "", "", 10, 0)
		require.NoError(t, err)
		require.Empty(t, kvs)
	})
}
