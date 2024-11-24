// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"os"
)

// ToDo: migrate to https://pkg.go.dev/os#Root.OpenRoot

func (b *Backend) Start(_ context.Context) error {
	err := os.MkdirAll(b.DataBasePath, DefaultDirectoryMode)
	if err != nil {
		return err
	}

	// https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	_, err = b.Create(context.Background(), "registry/health", []byte(`{"health":"true"}`), 0)
	return err
}

func (b *Backend) DbSize(_ context.Context) (int64, error) {
	return CalculateDirectorySize(b.DataBasePath)
}

func (b *Backend) CurrentRevision(_ context.Context) (int64, error) {
	return b.ReadCounter(), nil
}
