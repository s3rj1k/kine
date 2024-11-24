// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionCurrentRevision(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

	_, err := b.Create(ctx, "a", nil, 0)
	require.NoError(t, err)

	_, err = b.Create(ctx, "a/b", nil, 0)
	require.NoError(t, err)

	rev, err := b.CurrentRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), rev)

	_, err = b.Create(ctx, "a/b/c", nil, 0)
	require.NoError(t, err)

	_, err = b.Create(ctx, "b", nil, 1)
	require.NoError(t, err)

	rev, err = b.CurrentRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), rev)
}
