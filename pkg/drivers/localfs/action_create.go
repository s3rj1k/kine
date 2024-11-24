// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	ErrKeyCreate      = errors.New("failed to create key")
	ErrKeyCreateEmpty = errors.New("failed to create key, empty name")
)

func (b *Backend) Create(_ context.Context, key string, value []byte, lease int64) (int64 /*revision*/, error) {
	if key == "" {
		return b.ReadCounter(), ErrKeyCreateEmpty
	}

	err := os.MkdirAll(filepath.Join(b.DataBasePath, key), DefaultDirectoryMode)
	if err != nil {
		return b.ReadCounter(), errors.Join(fmt.Errorf("%w: %s", ErrKeyCreate, key), err)
	}

	var info Info

	info.Rev = b.IncrementCounter()
	info.Ctime = time.Now().Unix()

	if lease > 0 {
		info.Expires = info.Ctime + lease
	}

	err = WriteFile(
		filepath.Join(b.DataBasePath, key, info.String()),
		value,
		DefaultFileMode,
	)
	if err != nil {
		return info.Rev, errors.Join(fmt.Errorf("%w: %s", ErrKeyCreate, key), err)
	}

	return info.Rev, nil
}
