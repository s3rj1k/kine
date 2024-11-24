// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

const (
	DefaultCounterFileMode os.FileMode = 0o644

	DefaultCounterFilePath    = "counter"
	DefaultCounterOpenROFlags = os.O_RDONLY | syscall.O_NOATIME | syscall.O_NOFOLLOW
	DefaultCounterOpenRWFlags = os.O_RDWR | os.O_CREATE | os.O_SYNC | syscall.O_NOATIME | syscall.O_NOFOLLOW
)

func NoneThreadSafeReadCounter(counterFilePath string) int64 {
	fd, err := os.OpenFile(counterFilePath, DefaultCounterOpenROFlags, DefaultFileMode)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}

		return -1
	}

	defer func() {
		_ = fd.Close()
	}()

	var value int64

	err = binary.Read(fd, binary.BigEndian, &value)
	if err != nil {
		value = -1
	}

	return value
}

func NoneThreadSafeIncrementCounter(counterFilePath string) int64 {
	fd, err := os.OpenFile(counterFilePath, DefaultCounterOpenRWFlags, DefaultCounterFileMode)
	if err != nil {
		return -1
	}

	defer func() {
		_ = fd.Close()
	}()

	var value int64

	err = binary.Read(fd, binary.BigEndian, &value)
	if err != nil {
		value = -1
	}

	value++

	_, err = fd.Seek(0, 0)
	if err != nil {
		return -1
	}

	err = binary.Write(fd, binary.BigEndian, value)
	if err != nil {
		return -1
	}

	return value
}

func (b *Backend) IncrementCounter() int64 {
	b.counterLock.Lock()
	defer b.counterLock.Unlock()

	for {
		if value := NoneThreadSafeIncrementCounter(b.CounterFilePath); value > 0 {
			return value
		}
	}
}

func (b *Backend) ReadCounter() int64 {
	b.counterLock.Lock()
	defer b.counterLock.Unlock()

	for {
		if value := NoneThreadSafeReadCounter(b.CounterFilePath); value >= 0 {
			return value
		}
	}
}
