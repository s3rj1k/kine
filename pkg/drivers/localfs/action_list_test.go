// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionList(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

	// create keys
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

	// list all keys
	rev, kvs, err := b.List(ctx, "", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(7), rev)
	require.Equal(t, 7, len(kvs))
	requireKeysSorted(t, kvs)

	// list keys with prefix
	rev, kvs, err = b.List(ctx, "a", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(7), rev)
	require.Equal(t, 3, len(kvs))
	requireKeysSorted(t, kvs)

	// list the keys >= start key
	rev, kvs, err = b.List(ctx, "", "b", 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(7), rev)
	require.Equal(t, 4, len(kvs))
	requireKeysSorted(t, kvs)

	// list the keys up to a revision
	rev, kvs, err = b.List(ctx, "", "", 0, 3)
	require.NoError(t, err)
	require.Equal(t, int64(7), rev)
	require.Equal(t, 3, len(kvs))
	requireKeysSorted(t, kvs)
	requireKeys(t, kvs, "a", "a/b/c", "b")

	// list the keys with a limit
	rev, kvs, err = b.List(ctx, "", "", 4, 0)
	require.NoError(t, err)
	require.Equal(t, int64(7), rev)
	require.Equal(t, 4, len(kvs))
	requireKeysSorted(t, kvs)
	requireKeys(t, kvs, "a", "a/b", "a/b/c", "b")

	// list the keys with a limit after some start key
	rev, kvs, err = b.List(ctx, "", "b", 2, 0)
	require.NoError(t, err)
	require.Equal(t, int64(7), rev)
	require.Equal(t, 2, len(kvs))
	requireKeysSorted(t, kvs)
	requireKeys(t, kvs, "b", "c")
}
