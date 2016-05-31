package consistent_hash

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

var errNoNode = errors.New("consistent hash: no node exist")

type uints []uint32

func (arr uints) Len() int           { return len(arr) }
func (arr uints) Less(i, j int) bool { return arr[i] < arr[j] }
func (arr uints) Swap(i, j int)      { arr[i], arr[j] = arr[j], arr[i] }

type ConsistentHash struct {
	sync.RWMutex

	nodes         map[uint32]string
	points        uints
	virtualMultis int
}

func Default() *ConsistentHash {
	return New(20)
}

func New(virutalMultis int) *ConsistentHash {
	return &ConsistentHash{
		nodes:         make(map[uint32]string),
		virtualMultis: virutalMultis,
	}
}

func (c *ConsistentHash) Add(node string) {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < c.virtualMultis; i++ {
		point := c.hashKey(c.virtualPoint(node, i))
		c.nodes[point] = node
	}
	c.update()
}

func (c *ConsistentHash) Remove(node string) {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < c.virtualMultis; i++ {
		point := c.hashKey(c.virtualPoint(node, i))
		delete(c.nodes, point)
	}
	c.update()
}

func (c *ConsistentHash) Get(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.points) == 0 {
		return "", errNoNode
	}
	point := c.hashKey(key)
	return c.nodes[c.search(point)], nil
}

func (c *ConsistentHash) Nodes() map[uint32]string {
	c.RLock()
	defer c.RUnlock()
	return c.nodes
}

func (c *ConsistentHash) Clear() {
	c.Lock()
	defer c.Unlock()
	c.nodes = nil
	c.points = nil
}

func (c *ConsistentHash) update() {
	c.points = nil
	for k, _ := range c.nodes {
		c.points = append(c.points, k)
	}
	sort.Sort(c.points)
}

func (c *ConsistentHash) virtualPoint(node string, index int) string {
	return fmt.Sprintf("%s%d", node, index)
}

func (c *ConsistentHash) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *ConsistentHash) search(point uint32) uint32 {
	index := sort.Search(len(c.points), func(i int) bool {
		return c.points[i] >= point
	})
	if index < len(c.points) {
		return c.points[index]
	}
	return c.points[0]
}
