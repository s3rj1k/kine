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

func (*Backend) Create(_ context.Context, key string, value []byte, lease int64) (int64, error) {
	if key == "" {
		return 0, ErrKeyCreateEmpty
	}

	if err := os.MkdirAll(key, DefaultDirectoryMode); err != nil {
		return 0, errors.Join(fmt.Errorf("%w: %s", ErrKeyCreate, key), err)
	}

	var info Info

	info.Rev = IncrementCounter()
	info.Ctime = time.Now().Unix()

	if lease > 0 {
		info.Expires = info.Ctime + lease
	}

	if err := WriteFile(
		filepath.Join(key, info.String()),
		value,
		DefaultFileMode,
	); err != nil {
		return 0, errors.Join(fmt.Errorf("%w: %s", ErrKeyCreate, key), err)
	}

	return info.Rev, nil
}
