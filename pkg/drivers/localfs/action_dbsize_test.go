// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionDbSize(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

	_, err := b.Create(ctx, "a", []byte(`a`), 0)
	require.NoError(t, err)

	_, err = b.Create(ctx, "a/b", []byte(`a/b`), 0)
	require.NoError(t, err)

	size1, err := b.DbSize(ctx)
	require.NoError(t, err)
	require.Greater(t, size1, int64(0))

	_, err = b.Create(ctx, "a/b/c", []byte(`a/b/c`), 0)
	require.NoError(t, err)

	_, err = b.Create(ctx, "b", []byte(`b`), 1)
	require.NoError(t, err)

	size2, err := b.DbSize(ctx)
	require.NoError(t, err)
	require.Greater(t, size2, size1)
}
