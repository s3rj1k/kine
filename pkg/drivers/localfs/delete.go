// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/k3s-io/kine/pkg/server"
)

func (*Backend) Delete(_ context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	loc, info, err := getInfo(key, revision)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil, true, nil
		}

		return 0, nil, false, err
	}

	content, err := os.ReadFile(loc)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil, true, nil
		}

		return 0, nil, false, err
	}

	kv := &server.KeyValue{
		Key:         key,
		ModRevision: revision,
		Value:       content,
	}

	info.Expires = info.Ctime

	err = os.Rename(loc, filepath.Join(filepath.Dir(loc), info.String()))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil, true, nil
		}

		return 0, nil, false, err
	}

	return revision, kv, true, nil
}
