// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"context"
)

func (b *Backend) DbSize(_ context.Context) (totalSize int64, err error) {
	lsm, vlog := b.db.Size()

	return lsm + vlog, nil
}
