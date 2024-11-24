// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionUpdate(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

	_, err := b.Create(ctx, "a", []byte("b"), 1)
	require.NoError(t, err)

	rev, kv, ok, err := b.Update(ctx, "a", []byte("c"), 1, 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), rev)
	require.Equal(t, true, ok)
	require.Equal(t, "a", kv.Key)
	require.Equal(t, "c", string(kv.Value))
	require.Equal(t, int64(0), kv.Lease)
	require.Equal(t, int64(2), kv.ModRevision)
	require.Equal(t, int64(1), kv.CreateRevision)

	rev, kv, ok, err = b.Update(ctx, "a", []byte("d"), 2, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3), rev)
	require.Equal(t, true, ok)
	require.Equal(t, "a", kv.Key)
	require.Equal(t, "d", string(kv.Value))
	require.Equal(t, int64(1), kv.Lease)
	require.Equal(t, int64(3), kv.ModRevision)
	require.Equal(t, int64(1), kv.CreateRevision)

	// update with wrong revision
	rev, _, ok, err = b.Update(ctx, "a", []byte("e"), 42, 1)
	require.Error(t, err)
	require.Equal(t, int64(42), rev)
	require.Equal(t, false, ok)
}
