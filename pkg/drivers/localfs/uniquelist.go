// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"slices"
)

type UniqueSortedList[T comparable] struct {
	less func(a, b T) bool
	data *[]T
}

func (l *UniqueSortedList[T]) sortUnique() {
	slices.SortFunc(*l.data, func(a, b T) int {
		switch {
		case l.less(a, b):
			return -1
		case l.less(b, a):
			return 1
		default:
			return 0
		}
	})

	*l.data = slices.Compact(*l.data)
}

func (l *UniqueSortedList[T]) Data() []T {
	if l == nil || l.data == nil {
		return nil
	}

	l.sortUnique()

	return slices.Clone(*l.data)
}

func (l *UniqueSortedList[T]) Has(el T) bool {
	if l == nil || l.data == nil {
		return false
	}

	return slices.Contains(*l.data, el)
}

func (l *UniqueSortedList[T]) Add(el T) {
	if l == nil || l.data == nil {
		return
	}

	if !slices.Contains(*l.data, el) {
		*l.data = append(*l.data, el)
	}
}

func (l *UniqueSortedList[T]) Delete(el T) {
	if l == nil || l.data == nil {
		return
	}

	for {
		j := slices.Index(*l.data, el)
		if j < 0 {
			return
		}

		k := j + 1
		if k > len(*l.data) {
			k = len(*l.data)
		}

		*l.data = slices.Delete(*l.data, j, k)
	}
}

func (l *UniqueSortedList[T]) Range(f func(el T) (stop bool)) {
	if l == nil || l.data == nil {
		return
	}

	l.sortUnique()

	for _, el := range *l.data {
		if stop := f(el); stop {
			break
		}
	}
}

func (l *UniqueSortedList[T]) Size() int {
	if l == nil || l.data == nil {
		return 0
	}

	return len(*l.data)
}

func NewUniqueSortedList[T comparable](less func(a, b T) bool, el ...T) *UniqueSortedList[T] {
	ul := new(UniqueSortedList[T])
	data := make([]T, 0, len(el))

	ul.data = &data
	ul.less = less

	for _, el := range el {
		ul.Add(el)
	}

	return ul
}
