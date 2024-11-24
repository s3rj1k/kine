// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActionGet(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		rev1, err := b.Create(ctx, "a", []byte("b"), 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), rev1)

		// get key
		rev, kv, err := b.Get(ctx, "a", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), rev)
		require.NotNil(t, kv)
		require.Equal(t, "a", kv.Key)
		require.Equal(t, []byte("b"), kv.Value)
		require.Equal(t, int64(0), kv.Lease)
		require.Equal(t, int64(1), kv.ModRevision)
		require.Equal(t, int64(1), kv.CreateRevision)
	})

	t.Run("get_non_existent_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// get non-existent key
		rev, kv, err := b.Get(ctx, "does-not-exist", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), rev) // counter starts at 0
		require.Nil(t, kv)
	})

	t.Run("get_with_specific_revision", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create and update key multiple times
		rev1, err := b.Create(ctx, "evolving", []byte("v1"), 0)
		require.NoError(t, err)

		rev2, _, ok, err := b.Update(ctx, "evolving", []byte("v2"), rev1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		rev3, _, ok, err := b.Update(ctx, "evolving", []byte("v3"), rev2, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// get at specific revision
		currentRev, kv, err := b.Get(ctx, "evolving", "", 0, rev1)
		require.NoError(t, err)
		require.Equal(t, int64(3), currentRev) // current revision
		require.NotNil(t, kv)
		require.Equal(t, []byte("v1"), kv.Value)
		require.Equal(t, rev1, kv.ModRevision)

		// get at another revision
		currentRev, kv, err = b.Get(ctx, "evolving", "", 0, rev2)
		require.NoError(t, err)
		require.Equal(t, int64(3), currentRev)
		require.NotNil(t, kv)
		require.Equal(t, []byte("v2"), kv.Value)
		require.Equal(t, rev2, kv.ModRevision)

		// get latest
		currentRev, kv, err = b.Get(ctx, "evolving", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, int64(3), currentRev)
		require.NotNil(t, kv)
		require.Equal(t, []byte("v3"), kv.Value)
		require.Equal(t, rev3, kv.ModRevision)
	})

	t.Run("get_after_delete", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key
		rev1, err := b.Create(ctx, "temp", []byte("temporary"), 0)
		require.NoError(t, err)

		// verify key exists
		_, kv, err := b.Get(ctx, "temp", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("temporary"), kv.Value)

		// delete key
		rev2, _, deleted, err := b.Delete(ctx, "temp", rev1)
		require.NoError(t, err)
		require.True(t, deleted)

		// get after delete
		currentRev, kv, err := b.Get(ctx, "temp", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, rev2, currentRev)
		require.Nil(t, kv)
	})

	t.Run("get_with_lease", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key with lease
		_, err := b.Create(ctx, "leased", []byte("expires"), 3600)
		require.NoError(t, err)

		// get key with lease
		_, kv, err := b.Get(ctx, "leased", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("expires"), kv.Value)
		require.Equal(t, int64(3600), kv.Lease)
	})

	t.Run("get_expired_key", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create key with short lease
		rev1, err := b.Create(ctx, "short-lived", []byte("gone-soon"), 1)
		require.NoError(t, err)

		// verify key exists initially
		_, kv, err := b.Get(ctx, "short-lived", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("gone-soon"), kv.Value)

		// wait for expiration
		time.Sleep(2 * time.Second)

		// get expired key
		currentRev, kv, err := b.Get(ctx, "short-lived", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, rev1, currentRev)
		require.Nil(t, kv) // expired key should not be returned
	})

	t.Run("get_nested_keys", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create nested keys
		keys := map[string][]byte{
			"a":       []byte("root"),
			"a/b":     []byte("level1"),
			"a/b/c":   []byte("level2"),
			"a/b/c/d": []byte("level3"),
		}

		revisions := make(map[string]int64)

		for key, value := range keys {
			rev, err := b.Create(ctx, key, value, 0)
			require.NoError(t, err)

			revisions[key] = rev
		}

		// get each nested key
		for key, expectedValue := range keys {
			_, kv, err := b.Get(ctx, key, "", 0, 0)
			require.NoError(t, err)
			require.NotNil(t, kv)
			require.Equal(t, key, kv.Key)
			require.Equal(t, expectedValue, kv.Value)
			require.Equal(t, revisions[key], kv.CreateRevision)
		}
	})

	t.Run("get_with_special_characters", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create keys with special characters
		specialKeys := map[string][]byte{
			"key-with-dash":       []byte("dash"),
			"key_with_underscore": []byte("underscore"),
			"key.with.dots":       []byte("dots"),
			"key:with:colons":     []byte("colons"),
			"key=with=equals":     []byte("equals"),
		}

		for key, value := range specialKeys {
			_, err := b.Create(ctx, key, value, 0)
			require.NoError(t, err)
		}

		// get keys with special characters
		for key, expectedValue := range specialKeys {
			_, kv, err := b.Get(ctx, key, "", 0, 0)
			require.NoError(t, err)
			require.NotNil(t, kv)
			require.Equal(t, key, kv.Key)
			require.Equal(t, expectedValue, kv.Value)
		}
	})

	t.Run("get_at_revision_before_creation", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create a few keys
		rev1, err := b.Create(ctx, "first", []byte("1st"), 0)
		require.NoError(t, err)

		rev2, err := b.Create(ctx, "second", []byte("2nd"), 0)
		require.NoError(t, err)

		rev3, err := b.Create(ctx, "third", []byte("3rd"), 0)
		require.NoError(t, err)

		// get "third" at revision before it was created
		currentRev, kv, err := b.Get(ctx, "third", "", 0, rev2)
		require.NoError(t, err)
		require.Equal(t, rev3, currentRev) // current revision
		require.Nil(t, kv)                 // key didn't exist at rev2

		// get "second" at revision before it was created
		currentRev, kv, err = b.Get(ctx, "second", "", 0, rev1)
		require.NoError(t, err)
		require.Equal(t, rev3, currentRev)
		require.Nil(t, kv) // key didn't exist at rev1
	})

	t.Run("get_after_compact", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create and update key
		rev1, err := b.Create(ctx, "compact-test", []byte("v1"), 0)
		require.NoError(t, err)

		rev2, _, ok, err := b.Update(ctx, "compact-test", []byte("v2"), rev1, 0)
		require.NoError(t, err)
		require.True(t, ok)

		rev3, _, ok, err := b.Update(ctx, "compact-test", []byte("v3"), rev2, 0)
		require.NoError(t, err)
		require.True(t, ok)

		// compact up to rev2
		_, err = b.Compact(ctx, rev2)
		require.NoError(t, err)

		// get latest version
		_, kv, err := b.Get(ctx, "compact-test", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("v3"), kv.Value)
		require.Equal(t, rev3, kv.ModRevision)

		// get at compacted revision
		_, kv, err = b.Get(ctx, "compact-test", "", 0, rev1)
		require.NoError(t, err)
		require.Nil(t, kv) // compacted data should not be available
	})

	t.Run("get_with_various_options", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create a set of test keys with different values
		keys := []string{"a", "b", "c", "foo", "foo/abc", "fop"}
		for _, key := range keys {
			_, err := b.Create(ctx, key, []byte("value-"+key), 0)
			require.NoError(t, err)
		}

		// test basic get
		rev, kv, err := b.Get(ctx, "a", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, "a", kv.Key)
		require.Equal(t, []byte("value-a"), kv.Value)

		// get with specific revision
		initialRev := rev
		_, err = b.Create(ctx, "new-key", []byte("new-value"), 0)
		require.NoError(t, err)

		_, kv, err = b.Get(ctx, "new-key", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)

		// get with old revision should still find old keys
		_, kv, err = b.Get(ctx, "a", "", 0, initialRev)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, "a", kv.Key)

		// but should not find new keys
		_, kv, err = b.Get(ctx, "new-key", "", 0, initialRev)
		require.NoError(t, err)
		require.Nil(t, kv)

		// test getting non-existent keys
		_, kv, err = b.Get(ctx, "does-not-exist", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)

		// update a key value and verify revision
		origRev, origKv, err := b.Get(ctx, "c", "", 0, 0)
		require.NoError(t, err)

		newRev, _, updated, err := b.Update(ctx, "c", []byte("updated-c"), origKv.ModRevision, 0)
		require.NoError(t, err)
		require.True(t, updated)

		// get the latest version
		_, kv, err = b.Get(ctx, "c", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("updated-c"), kv.Value)
		require.Equal(t, newRev, kv.ModRevision)

		// get the original version
		_, kv, err = b.Get(ctx, "c", "", 0, origRev)
		require.NoError(t, err)
		require.Equal(t, []byte("value-c"), kv.Value)
		require.Equal(t, origKv.ModRevision, kv.ModRevision)

		// test with expired key
		_, err = b.Create(ctx, "expiring", []byte("will-expire"), 1) // expires in 1 second
		require.NoError(t, err)

		// key should exist initially
		_, kv, err = b.Get(ctx, "expiring", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, "expiring", kv.Key)

		// wait for key to expire
		time.Sleep(2 * time.Second)

		// key should no longer exist after expiration
		_, kv, err = b.Get(ctx, "expiring", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)
	})

	t.Run("get_after_operations", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create a key
		rev1, err := b.Create(ctx, "test-key", []byte("original"), 0)
		require.NoError(t, err)

		// get the key and verify
		_, kv, err := b.Get(ctx, "test-key", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("original"), kv.Value)

		// update the key
		rev2, _, updated, err := b.Update(ctx, "test-key", []byte("updated"), rev1, 0)
		require.NoError(t, err)
		require.True(t, updated)

		// get the latest version
		_, kv, err = b.Get(ctx, "test-key", "", 0, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("updated"), kv.Value)
		require.Equal(t, rev2, kv.ModRevision)

		// get the original version
		_, kv, err = b.Get(ctx, "test-key", "", 0, rev1)
		require.NoError(t, err)
		require.Equal(t, []byte("original"), kv.Value)
		require.Equal(t, rev1, kv.ModRevision)

		// delete the key
		_, _, deleted, err := b.Delete(ctx, "test-key", 0)
		require.NoError(t, err)
		require.True(t, deleted)

		// key should no longer exist
		_, kv, err = b.Get(ctx, "test-key", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)

		// create another key
		_, err = b.Create(ctx, "compact-test", []byte("before-compact"), 0)
		require.NoError(t, err)

		// note current revision
		compactRev, _, err := b.Get(ctx, "compact-test", "", 0, 0)
		require.NoError(t, err)

		// compact the database
		_, err = b.Compact(ctx, compactRev)
		require.NoError(t, err)

		// key should still exist after compaction
		_, kv, err = b.Get(ctx, "compact-test", "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.Equal(t, []byte("before-compact"), kv.Value)
	})

	t.Run("get_with_nested_keys", func(t *testing.T) {
		// setup fresh backend
		b := setupBackend(t)
		ctx := context.Background()

		// create nested keys
		nestedKeys := map[string]string{
			"parent":             "parent-value",
			"parent/child":       "child-value",
			"parent/child/grand": "grandchild-value",
			"another/path":       "another-value",
		}

		for key, value := range nestedKeys {
			_, err := b.Create(ctx, key, []byte(value), 0)
			require.NoError(t, err)
		}

		// get each key and verify
		for key, expectedValue := range nestedKeys {
			_, kv, err := b.Get(ctx, key, "", 0, 0)
			require.NoError(t, err)
			require.NotNil(t, kv)
			require.Equal(t, key, kv.Key)
			require.Equal(t, []byte(expectedValue), kv.Value)
		}

		// test getting a non-existent nested key
		_, kv, err := b.Get(ctx, "parent/nonexistent", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)

		// test getting a partial path
		_, kv, err = b.Get(ctx, "parent/child/nonexistent", "", 0, 0)
		require.NoError(t, err)
		require.Nil(t, kv)
	})
}
