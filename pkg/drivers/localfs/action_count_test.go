// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionCount(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

	_, err := b.Create(ctx, "a", nil, 0)
	require.NoError(t, err)

	_, err = b.Create(ctx, "a/b", nil, 0)
	require.NoError(t, err)

	rev, count, err := b.Count(ctx, "", "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), rev)
	require.Equal(t, int64(2), count)

	_, err = b.Create(ctx, "a/b/c", nil, 0)
	require.NoError(t, err)

	_, err = b.Create(ctx, "b", nil, 1)
	require.NoError(t, err)

	rev, count, err = b.Count(ctx, "", "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(4), rev)
	require.Equal(t, int64(4), count)

	rev, count, err = b.Count(ctx, "foobar", "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(4), rev)
	require.Equal(t, int64(0), count)
}
