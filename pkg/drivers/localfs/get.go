// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/k3s-io/kine/pkg/server"
)

var (
	ErrFileNotFound    = errors.New("blob not found")
	ErrInvalidFilename = errors.New("invalid name format")
)

func filterNames(names []string, revision int64) (count int64, info Info) {
	for i := range names {
		val, err := NewInfo(names[i])
		if err != nil {
			continue
		}

		if revision != 0 && revision == info.Rev {
			break
		}

		if val.Expires != 0 && val.Expires <= val.Ctime {
			continue
		}

		info = val
		count++
	}

	return count, info
}

func getInfo(key string, revision int64) (string, Info, error) {
	var info Info

	names, err := ReadDirNames(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", info, ErrFileNotFound
		}

		return "", info, err
	}

	_, info = filterNames(names, revision)

	return filepath.Join(key, info.String()), info, nil
}

func (*Backend) Get(_ context.Context, key, _ string, _, revision int64) (int64, *server.KeyValue, error) {
	loc, info, err := getInfo(key, revision)
	if err != nil {
		return revision, nil, err
	}

	content, err := os.ReadFile(loc)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return revision, nil, ErrFileNotFound
		}

		return revision, nil, err
	}

	kv := &server.KeyValue{
		Key:            key,
		CreateRevision: info.Rev,
		ModRevision:    info.Rev,
		Value:          content,
		Lease:          info.Expires,
	}

	return info.Rev, kv, nil
}
