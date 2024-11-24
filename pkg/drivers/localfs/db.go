// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"os"
)

func getDataBaseDirectory() string {
	return os.Getenv(DataBasePathEnvironKey)
}

func (*Backend) Start(_ context.Context) error {
	dbDirectory := getDataBaseDirectory()

	err := os.MkdirAll(dbDirectory, DefaultDirectoryMode)
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
