// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"os"
)

func getDataBaseDirectory() string {
	return os.Getenv(DataBasePathEnvironKey)
}

func (b *Backend) Start(_ context.Context) error {
	dbDirectory := getDataBaseDirectory()

	err := os.MkdirAll(dbDirectory, DefaultDirectoryMode)
	if err != nil {
		return err
	}

	err = os.Chdir(dbDirectory)
	if err != nil {
		return err
	}

	// https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	_, err = b.Create(context.Background(), "/registry/health", []byte(`{"health":"true"}`), 0)
	if err != nil {
		return err
	}

	return os.Chdir(dbDirectory)
}

func (*Backend) DbSize(_ context.Context) (int64, error) {
	return CalculateDirectorySize(getDataBaseDirectory())
}

func (*Backend) CurrentRevision(_ context.Context) (int64, error) {
	return ReadCounter(), nil
}
