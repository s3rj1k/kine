// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/k3s-io/kine/pkg/server"
)

func TestActionCreate(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create first key
		rev, err := b.Create(ctx, "a", nil, 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), rev)

		// attempt to create same key again
		_, err = b.Create(ctx, "a", nil, 0)
		require.ErrorIs(t, err, server.ErrKeyExists)

		// create nested key
		rev, err = b.Create(ctx, "a/b", nil, 0)
		require.NoError(t, err)
		require.Equal(t, int64(2), rev)

		// create deeply nested key
		rev, err = b.Create(ctx, "a/b/c", nil, 0)
		require.NoError(t, err)
		require.Equal(t, int64(3), rev)

		// create key with lease
		rev, err = b.Create(ctx, "b", nil, 1)
		require.NoError(t, err)
		require.Equal(t, int64(4), rev)
	})

	t.Run("create_with_values", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys with different values
		rev1, err := b.Create(ctx, "binary", []byte{0x00, 0xFF, 0x42}, 0)
		require.NoError(t, err)

		rev2, err := b.Create(ctx, "text", []byte("Hello, World!"), 0)
		require.NoError(t, err)

		rev3, err := b.Create(ctx, "json", []byte(`{"key":"value"}`), 0)
		require.NoError(t, err)

		// verify values were stored correctly
		_, kv, err := b.Get(ctx, "binary", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, []byte{0x00, 0xFF, 0x42}, kv.Value)
		require.Equal(t, rev1, kv.CreateRevision)

		_, kv, err = b.Get(ctx, "text", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, World!"), kv.Value)
		require.Equal(t, rev2, kv.CreateRevision)

		_, kv, err = b.Get(ctx, "json", "", 0, 0)
		require.NoError(t, err)
		require.JSONEq(t, `{"key":"value"}`, string(kv.Value))
		require.Equal(t, rev3, kv.CreateRevision)
	})

	t.Run("create_with_lease", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key with lease
		_, err := b.Create(ctx, "temp", []byte("expires"), 2)
		require.NoError(t, err)

		// create key without lease
		rev2, err := b.Create(ctx, "permanent", []byte("forever"), 0)
		require.NoError(t, err)

		// verify lease is stored correctly
		_, kv, err := b.Get(ctx, "temp", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(2), kv.Lease)

		_, kv, err = b.Get(ctx, "permanent", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), kv.Lease)

		// wait for lease to expire
		time.Sleep(3 * time.Second)

		// expired key should not be accessible
		_, kv, err = b.Get(ctx, "temp", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)

		// permanent key should still exist
		_, kv, err = b.Get(ctx, "permanent", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("forever"), kv.Value)

		// creating the same key after expiration should succeed
		rev3, err := b.Create(ctx, "temp", []byte("new-value"), 0)
		require.NoError(t, err)
		require.Greater(t, rev3, rev2)
	})

	t.Run("create_nested_paths", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create deeply nested keys
		paths := []string{
			"root",
			"root/level1",
			"root/level1/level2",
			"root/level1/level2/level3",
			"root/level1/level2/level3/level4",
			"root/alternative/path",
		}

		revisions := make([]int64, len(paths))

		for i, path := range paths {
			rev, err := b.Create(ctx, path, []byte(path), 0)
			require.NoError(t, err)

			revisions[i] = rev
		}

		// verify all keys exist with correct values
		for i, path := range paths {
			_, kv, err := b.Get(ctx, path, "", 0, 0)
			require.NoError(t, err)
			require.Equal(t, []byte(path), kv.Value)
			require.Equal(t, revisions[i], kv.CreateRevision)
			require.Equal(t, revisions[i], kv.ModRevision)
		}

		// verify hierarchy doesn't affect other keys
		_, err := b.Create(ctx, "separate/tree", []byte("different"), 0)
		require.NoError(t, err)

		// count all keys
		_, count, err := b.Count(ctx, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, int64(len(paths)+1), count)
	})

	t.Run("create_with_special_characters", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create with special characters in key
		specialKeys := []string{
			"key-with-dash",
			"key_with_underscore",
			"key.with.dots",
			"key:with:colons",
			"key=with=equals",
		}

		for _, key := range specialKeys {
			_, err := b.Create(ctx, key, []byte(key), 0)
			require.NoError(t, err)
		}

		// verify all special keys exist
		for _, key := range specialKeys {
			_, kv, err := b.Get(ctx, key, "", 0, 0)
			require.NoError(t, err)
			require.Equal(t, []byte(key), kv.Value)
		}
	})

	t.Run("create_after_delete", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		rev1, err := b.Create(ctx, "reusable", []byte("original"), 0)
		require.NoError(t, err)

		// delete key
		rev2, deletedKv, deleted, err := b.Delete(ctx, "reusable", rev1)
		require.NoError(t, err)
		require.True(t, deleted)
		require.NotNil(t, deletedKv)
		require.Equal(t, []byte("original"), deletedKv.Value)

		// recreate key
		rev3, err := b.Create(ctx, "reusable", []byte("recreated"), 0)
		require.NoError(t, err)
		require.Greater(t, rev3, rev2)

		// verify new key exists
		_, kv, err := b.Get(ctx, "reusable", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("recreated"), kv.Value)
		require.Equal(t, rev3, kv.CreateRevision)
		require.Equal(t, rev3, kv.ModRevision)
	})
}
