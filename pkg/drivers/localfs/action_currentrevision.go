// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
)

func (b *Backend) currentRevision(_ context.Context) (int64, error) {
	return b.ReadCounter(), nil
}

func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	return b.currentRevision(ctx)
}
