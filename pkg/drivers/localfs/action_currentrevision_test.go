// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionCurrentRevision(t *testing.T) {
	// setup fresh backend
	b := setupBackend(t)
	ctx := context.Background()

	// create first key
	_, err := b.Create(ctx, "a", nil, 0)
	require.NoError(t, err)

	// get revision
	rev, err := b.CurrentRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), rev)

	// create second key
	_, err = b.Create(ctx, "a/b", nil, 0)
	require.NoError(t, err)

	// get revision
	rev, err = b.CurrentRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), rev)

	// create more keys
	_, err = b.Create(ctx, "a/b/c", nil, 0)
	require.NoError(t, err)

	_, err = b.Create(ctx, "b", nil, 1)
	require.NoError(t, err)

	// get revision
	rev, err = b.CurrentRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), rev)
}
