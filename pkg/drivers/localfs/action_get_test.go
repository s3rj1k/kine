// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionGet(t *testing.T) {
	b := setupBackend(t)

	ctx := context.Background()

	_, err := b.Create(ctx, "a", []byte("b"), 0)
	require.NoError(t, err)

	rev, ent, err := b.Get(ctx, "a", "", 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), rev)
	require.Equal(t, "a", ent.Key)
	require.Equal(t, "b", string(ent.Value))
	require.Equal(t, int64(0), ent.Lease)
	require.Equal(t, int64(1), ent.ModRevision)
	require.Equal(t, int64(1), ent.CreateRevision)
}
