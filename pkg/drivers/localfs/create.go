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

func (*Backend) Create(_ context.Context, key string, value []byte, lease int64) (revision int64, err error) {
	defer func() {
		revision = ReadCounter()
	}()

	if key == "" {
		return revision, ErrKeyCreateEmpty
	}

	dbDirectory := getDataBaseDirectory()

	err = os.MkdirAll(filepath.Join(dbDirectory, key), DefaultDirectoryMode)
	if err != nil {
		return revision, errors.Join(fmt.Errorf("%w: %s", ErrKeyCreate, key), err)
	}

	var info Info

	info.Rev = IncrementCounter()
	info.Ctime = time.Now().Unix()

	if lease > 0 {
		info.Expires = info.Ctime + lease
	}

	err = WriteFile(
		filepath.Join(dbDirectory, key, info.String()),
		value,
		DefaultFileMode,
	)
	if err != nil {
		return revision, errors.Join(fmt.Errorf("%w: %s", ErrKeyCreate, key), err)
	}

	return info.Rev, nil
}
