// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionDelete(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

	// create with lease
	_, err := b.Create(ctx, "a", []byte("b"), 1)
	require.NoError(t, err)

	// perform KV delete
	rev, kv, ok, err := b.Delete(ctx, "a", 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), rev) // delete creates a new revision
	require.Equal(t, true, ok)
	require.Equal(t, "a", kv.Key)
	require.Equal(t, "b", string(kv.Value))
	require.Equal(t, int64(1), kv.Lease)
	require.Equal(t, int64(2), kv.ModRevision)
	require.Equal(t, int64(1), kv.CreateRevision)

	// create again
	_, err = b.Create(ctx, "a", []byte("b"), 0)
	require.NoError(t, err)

	// delete expired revision
	rev, kv, ok, err = b.Delete(ctx, "a", 1)
	require.NoError(t, err)
	require.Equal(t, int64(3), rev) // should return current counter
	require.Equal(t, true, ok)
	require.Nil(t, kv) // no kv returned for already deleted entry

	// no revision, will delete the latest
	rev, _, ok, err = b.Delete(ctx, "a", 0)
	require.Equal(t, int64(4), rev) // delete creates a new revision
	require.Equal(t, true, ok)
	require.ErrorIs(t, nil, err)
}
