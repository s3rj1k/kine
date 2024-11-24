// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"context"
)

func (b *Backend) CurrentRevision(_ context.Context) (int64, error) {
	return int64(b.db.MaxVersion()), nil
}
