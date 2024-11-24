// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

const (
	UnixPathSeparator = "/"

	DefaultFileMode          os.FileMode = 0o444
	DefaultDirectoryMode     os.FileMode = 0o755
	DefaultDataFileOpenFlags             = os.O_WRONLY | os.O_CREATE | os.O_SYNC | syscall.O_NOATIME | syscall.O_NOFOLLOW | syscall.O_EXCL
)

func WriteFile(name string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(name, DefaultDataFileOpenFlags, perm)
	if err != nil {
		return err
	}

	_, err = f.Write(data)

	if errClose := f.Close(); errClose != nil && err == nil {
		err = errClose
	}

	return err
}

func ReadDirNames(loc string) ([]string, error) {
	f, err := os.Open(loc)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	names, err := f.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	slices.Sort(names)

	return names, nil
}

func CalculateDirectorySize(loc string) (totalSize int64, err error) {
	err = filepath.Walk(loc, func(path string, info os.FileInfo, walkErr error) error {
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
