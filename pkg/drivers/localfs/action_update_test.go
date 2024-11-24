// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/k3s-io/kine/pkg/server"
)

func TestActionUpdate(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key with lease
		rev1, err := b.Create(ctx, "a", []byte("b"), 1)
		require.NoError(t, err)
		require.Equal(t, int64(1), rev1)

		// update with matching revision
		rev2, kv, ok, err := b.Update(ctx, "a", []byte("c"), rev1, 0)
		require.NoError(t, err)
		require.Equal(t, int64(2), rev2)
		require.True(t, ok)
		require.NotNil(t, kv)
		require.Equal(t, "a", kv.Key)
		require.Equal(t, []byte("c"), kv.Value)
		require.Equal(t, int64(0), kv.Lease)
		require.Equal(t, int64(2), kv.ModRevision)
		require.Equal(t, int64(1), kv.CreateRevision)

		// update again with new lease
		rev3, kv, ok, err := b.Update(ctx, "a", []byte("d"), rev2, 1)
		require.NoError(t, err)
		require.Equal(t, int64(3), rev3)
		require.True(t, ok)
		require.NotNil(t, kv)
		require.Equal(t, "a", kv.Key)
		require.Equal(t, []byte("d"), kv.Value)
		require.Equal(t, int64(1), kv.Lease)
		require.Equal(t, int64(3), kv.ModRevision)
		require.Equal(t, int64(1), kv.CreateRevision)
	})

	t.Run("update_with_wrong_revision", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		_, err := b.Create(ctx, "test", []byte("initial"), 0)
		require.NoError(t, err)

		// update with wrong revision
		rev2, kv, ok, err := b.Update(ctx, "test", []byte("updated"), 42, 0)
		require.Error(t, err)
		require.Equal(t, server.ErrFutureRev, err)
		require.Equal(t, int64(42), rev2)
		require.False(t, ok)
		require.Nil(t, kv)

		// verify original value unchanged
		_, kv, err = b.Get(ctx, "test", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("initial"), kv.Value)
	})

	t.Run("update_non_existent_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// update non-existent key
		rev, kv, ok, err := b.Update(ctx, "does-not-exist", []byte("value"), 0, 0)
		require.Error(t, err)
		require.Equal(t, server.ErrNotSupported, err)
		require.Equal(t, int64(0), rev)
		require.False(t, ok)
		require.Nil(t, kv)
	})

	t.Run("update_with_zero_revision", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		_, err := b.Create(ctx, "flexible", []byte("v1"), 0)
		require.NoError(t, err)

		// update with revision 0 (any revision)
		rev2, kv, ok, err := b.Update(ctx, "flexible", []byte("v2"), 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(2), rev2)
		require.True(t, ok)
		require.Equal(t, []byte("v2"), kv.Value)
		require.Equal(t, int64(2), kv.ModRevision)
	})

	t.Run("update_after_delete", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		rev1, err := b.Create(ctx, "temp", []byte("original"), 0)
		require.NoError(t, err)

		// delete key
		_, _, deleted, err := b.Delete(ctx, "temp", rev1)
		require.NoError(t, err)
		require.True(t, deleted)

		// update deleted key
		rev3, kv, ok, err := b.Update(ctx, "temp", []byte("new"), 0, 0)
		require.Error(t, err)
		require.Equal(t, server.ErrNotSupported, err)
		require.Equal(t, int64(0), rev3)
		require.False(t, ok)
		require.Nil(t, kv)
	})

	t.Run("update_expired_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key with short lease
		_, err := b.Create(ctx, "expiring", []byte("soon-gone"), 1)
		require.NoError(t, err)

		// wait for expiration
		time.Sleep(2 * time.Second)

		// update expired key
		rev2, kv, ok, err := b.Update(ctx, "expiring", []byte("updated"), 0, 0)
		require.Error(t, err)
		require.Equal(t, server.ErrNotSupported, err)
		require.Equal(t, int64(0), rev2)
		require.False(t, ok)
		require.Nil(t, kv)
	})

	t.Run("update_nested_keys", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create nested keys
		keys := []string{
			"a/b/c",
			"a/b/d",
			"a/e",
		}

		revisions := make([]int64, len(keys))

		for i, key := range keys {
			rev, err := b.Create(ctx, key, []byte("initial"), 0)
			require.NoError(t, err)

			revisions[i] = rev
		}

		// update nested keys
		for i, key := range keys {
			newRev, kv, ok, err := b.Update(ctx, key, []byte("updated"), revisions[i], 0)
			require.NoError(t, err)
			require.True(t, ok)
			require.Greater(t, newRev, revisions[i])
			require.Equal(t, []byte("updated"), kv.Value)
			require.Equal(t, key, kv.Key)
		}
	})

	t.Run("update_lease_changes", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key without lease
		rev1, err := b.Create(ctx, "lease-test", []byte("no-lease"), 0)
		require.NoError(t, err)

		// verify no lease
		_, kv, err := b.Get(ctx, "lease-test", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), kv.Lease)

		// update to add lease
		rev2, kv, ok, err := b.Update(ctx, "lease-test", []byte("with-lease"), rev1, 3600)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(3600), kv.Lease)
		require.Equal(t, []byte("with-lease"), kv.Value)

		// update to remove lease
		_, kv, ok, err = b.Update(ctx, "lease-test", []byte("no-lease-again"), rev2, 0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(0), kv.Lease)
		require.Equal(t, []byte("no-lease-again"), kv.Value)
	})

	t.Run("update_special_characters", func(t *testing.T) {
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
			rev, err := b.Create(ctx, key, []byte("original"), 0)
			require.NoError(t, err)

			revisions[i] = rev
		}

		// update keys with special characters
		for i, key := range specialKeys {
			newRev, kv, ok, err := b.Update(ctx, key, []byte("updated"), revisions[i], 0)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, []byte("updated"), kv.Value)
			require.Equal(t, key, kv.Key)
			require.Greater(t, newRev, revisions[i])
		}
	})

	t.Run("update_preserves_create_revision", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		rev1, err := b.Create(ctx, "preserve", []byte("v1"), 0)
		require.NoError(t, err)

		// get original create revision
		_, originalKv, err := b.Get(ctx, "preserve", "", 0, 0)
		require.NoError(t, err)

		originalCreateRev := originalKv.CreateRevision

		// update multiple times
		revisions := []int64{rev1}
		for i := 2; i <= 5; i++ {
			newRev, kv, ok, err := b.Update(ctx, "preserve", []byte(string(rune('0'+i))), revisions[i-2], 0)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, originalCreateRev, kv.CreateRevision)
			require.Greater(t, kv.ModRevision, originalCreateRev)

			revisions = append(revisions, newRev)
		}

		// verify create revision is preserved
		_, finalKv, err := b.Get(ctx, "preserve", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, originalCreateRev, finalKv.CreateRevision)
		require.Equal(t, revisions[len(revisions)-1], finalKv.ModRevision)
	})
}
