// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var (
	keyPart = regexp.MustCompile(`^[a-z0-9][-a-z0-9.]*[a-z0-9]$`)

	ErrInvalidKeyFormat = errors.New("invalid key format")
)

type Info struct {
	Rev     int64
	Ctime   int64
	Expires int64
}

func NewInfo(loc string) (info Info, err error) {
	parts := strings.Split(filepath.Base(loc), ".")
	if len(parts) < 3 {
		return info, ErrInvalidFilename
	}

	info.Rev, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return info, ErrInvalidFilename
	}

	info.Ctime, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return info, ErrInvalidFilename
	}

	info.Expires, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return info, ErrInvalidFilename
	}

	return info, nil
}

func (b Info) String() string {
	return fmt.Sprintf("%020d.%d.%d", b.Rev, b.Ctime, b.Expires)
}

func NormalizeKey(baseDir, key string) (string, error) {
	before, after, found := strings.Cut(filepath.Clean(key), UnixPathSeparator)
	if !found {
		if !keyPart.MatchString(before) {
			return "", ErrInvalidKeyFormat
		}

		return filepath.Join(baseDir, "default", before), nil
	}

	if !keyPart.MatchString(before) || !keyPart.MatchString(after) {
		return "", ErrInvalidKeyFormat
	}

	return filepath.Join(baseDir, before, after), nil
}
