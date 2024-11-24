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

func (*Backend) Create(_ context.Context, key string, value []byte, lease int64) (rev int64, err error) {
	defer func() {
		rev = ReadCounter()
	}()

	if key == "" {
		return rev, ErrKeyCreateEmpty
	}

	err = os.MkdirAll(key, DefaultDirectoryMode)
	if err != nil {
		return rev, errors.Join(fmt.Errorf("%w: %s", ErrKeyCreate, key), err)
	}

	var info Info

	info.Rev = IncrementCounter()
	info.Ctime = time.Now().Unix()

	if lease > 0 {
		info.Expires = info.Ctime + lease
	}

	err = WriteFile(
		filepath.Join(key, info.String()),
		value,
		DefaultFileMode,
	)
	if err != nil {
		return rev, errors.Join(fmt.Errorf("%w: %s", ErrKeyCreate, key), err)
	}

	return info.Rev, nil
}
