package consistent_hash

import "errors"

var (
	ErrNoNode           = errors.New("no node exist")
	ErrNodeAlreadyExist = errors.New("node already exist")
	ErrNodeNotFound     = errors.New("node not found")
)

type uints []uint32

func (arr uints) Len() int           { return len(arr) }
func (arr uints) Less(i, j int) bool { return arr[i] < arr[j] }
func (arr uints) Swap(i, j int)      { arr[i], arr[j] = arr[j], arr[i] }
