// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionDbSize(t *testing.T) {
	// setup fresh backend
	b := setupBackend(t)
	ctx := context.Background()

	// create key with small value
	_, err := b.Create(ctx, "a", []byte(`a`), 0)
	require.NoError(t, err)

	// create nested key with slightly larger value
	_, err = b.Create(ctx, "a/b", []byte(`a/b`), 0)
	require.NoError(t, err)

	// get database size after initial creates
	size1, err := b.DbSize(ctx)
	require.NoError(t, err)
	require.Positive(t, size1)

	// create another nested key with more content
	_, err = b.Create(ctx, "a/b/c", []byte(`a/b/c`), 0)
	require.NoError(t, err)

	// create key with lease
	_, err = b.Create(ctx, "b", []byte(`b`), 1)
	require.NoError(t, err)

	// get database size after more creates
	size2, err := b.DbSize(ctx)
	require.NoError(t, err)
	require.Greater(t, size2, size1)
}
