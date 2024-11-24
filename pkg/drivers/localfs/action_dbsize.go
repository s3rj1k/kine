// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"os"
	"path/filepath"
)

func (b *Backend) dbSize(_ context.Context) (totalSize int64, err error) {
	err = filepath.Walk(b.DataBasePath, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if info.Mode().IsRegular() {
			totalSize += info.Size()
		}

		return nil
	})

	return totalSize, err
}

func (b *Backend) DbSize(ctx context.Context) (totalSize int64, err error) {
	return b.dbSize(ctx)
}
