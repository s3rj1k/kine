// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActionCount(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create initial key
		_, err := b.Create(ctx, "a", nil, 0)
		require.NoError(t, err)

		// create nested key
		_, err = b.Create(ctx, "a/b", nil, 0)
		require.NoError(t, err)

		// count all keys
		rev, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(2), rev)
		require.Equal(t, int64(2), count)

		// create another nested key
		_, err = b.Create(ctx, "a/b/c", nil, 0)
		require.NoError(t, err)

		// create a key with a short lease
		_, err = b.Create(ctx, "b", nil, 1)
		require.NoError(t, err)

		// count all keys
		rev, count, err = b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(4), rev)
		require.Equal(t, int64(4), count)

		// count non-existent prefix
		rev, count, err = b.Count(ctx, "foobar", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(4), rev)   // revision is still current
		require.Equal(t, int64(0), count) // but no keys match
	})

	t.Run("count_with_prefix_filtering", func(t *testing.T) {
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

		// count all keys
		_, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(5), count)

		// count keys with "users" prefix
		_, count, err = b.Count(ctx, "users", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(3), count)

		// count keys with "config" prefix
		_, count, err = b.Count(ctx, "config", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(2), count)

		// count keys with more specific prefix
		_, count, err = b.Count(ctx, "users/admin", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	})

	t.Run("count_with_startkey", func(t *testing.T) {
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

		// count all keys
		_, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(5), count)

		// count starting from key_c (inclusive)
		_, count, err = b.Count(ctx, "", "key_c", 0)
		require.NoError(t, err)
		require.Equal(t, int64(3), count) // key_c, key_d, key_e

		// count with both prefix and startKey
		_, count, err = b.Count(ctx, "key", "key_b", 0)
		require.NoError(t, err)
		require.Equal(t, int64(4), count) // key_b through key_e

		// startKey that doesn't exist - should start from next available
		_, count, err = b.Count(ctx, "", "key_bb", 0)
		require.NoError(t, err)
		require.Equal(t, int64(5), count) // key_c, key_d, key_e
	})

	t.Run("count_with_revision", func(t *testing.T) {
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

		// count at latest revision
		rev, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, rev3, rev)
		require.Equal(t, int64(3), count)

		// count at specific revision
		rev, count, err = b.Count(ctx, "", "", rev2)
		require.NoError(t, err)
		require.Equal(t, rev3, rev)       // current revision is still returned
		require.Equal(t, int64(2), count) // but count is as of rev2

		// count at specific revision
		rev, count, err = b.Count(ctx, "", "", rev1)
		require.NoError(t, err)
		require.Equal(t, rev3, rev)
		require.Equal(t, int64(1), count)

		// update a key
		rev4, _, ok, err := b.Update(ctx, "r1", []byte("v1_updated"), rev1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// count all keys
		rev, count, err = b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, rev4, rev)
		require.Equal(t, int64(3), count)
	})

	t.Run("count_with_expired_keys", func(t *testing.T) {
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

		// count all keys
		rev, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(4), count)

		// wait for keys to expire
		time.Sleep(2 * time.Second)

		// count all keys
		rev2, count2, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, rev, rev2)
		require.Equal(t, int64(2), count2)
	})

	t.Run("count_after_deletes", func(t *testing.T) {
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
		rev4, err := b.Create(ctx, "keep1", []byte("k1"), 0)
		require.NoError(t, err)

		// count all keys
		_, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(4), count)

		// delete some keys
		_, _, deleted, err := b.Delete(ctx, "del1", rev1)
		require.NoError(t, err)
		require.True(t, deleted)

		rev6, _, deleted, err := b.Delete(ctx, "del2", rev2)
		require.NoError(t, err)
		require.True(t, deleted)

		// count all keys
		rev, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, rev6, rev)
		require.Equal(t, int64(2), count) // only del3 and keep1 remain

		// count at revision
		rev, count, err = b.Count(ctx, "", "", rev4)
		require.NoError(t, err)
		require.Equal(t, rev6, rev)
		require.Equal(t, int64(2), count)
	})

	t.Run("count_with_compaction", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys across multiple revisions
		_, err := b.Create(ctx, "compact1", []byte("c1"), 0)
		require.NoError(t, err)
		rev2, err := b.Create(ctx, "compact2", []byte("c2"), 0)
		require.NoError(t, err)
		rev3, err := b.Create(ctx, "compact3", []byte("c3"), 0)
		require.NoError(t, err)

		// count all keys
		_, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(3), count)

		// compact up to rev2
		_, err = b.Compact(ctx, rev2)
		require.NoError(t, err)

		// count all keys
		rev, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, rev3, rev)
		require.Equal(t, int64(1), count) // only compact3 remains
	})
}
