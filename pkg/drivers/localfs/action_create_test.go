// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestActionCreate(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

	rev, err := b.Create(ctx, "a", nil, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), rev)

	_, err = b.Create(ctx, "a", nil, 0)
	require.ErrorIs(t, err, server.ErrKeyExists)

	rev, err = b.Create(ctx, "a/b", nil, 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), rev)

	rev, err = b.Create(ctx, "a/b/c", nil, 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), rev)

	rev, err = b.Create(ctx, "b", nil, 1)
	require.NoError(t, err)
	require.Equal(t, int64(4), rev)
}
