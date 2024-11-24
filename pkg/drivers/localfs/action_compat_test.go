// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionCompact(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

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

	_, kvs, err := b.List(ctx, "", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, 7, len(kvs))

	_, err = b.Compact(ctx, 3)
	require.NoError(t, err)

	_, kvs, err = b.List(ctx, "", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(kvs))

	_, err = b.Compact(ctx, 7)
	require.NoError(t, err)

	_, kvs, err = b.List(ctx, "", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(kvs))
}
