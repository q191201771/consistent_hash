package consistent_hash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

type ConsistentHash interface {
	Exist(node string) bool
	Add(node string) error
	Remove(node string) error
	Get(key string) (string, error)
	Nodes() []string
	Clear()
}

type consistentHash struct {
	sync.RWMutex

	nodes         map[string]bool
	point2node    map[uint32]string
	points        uints
	virtualMultis int
}

func Default() ConsistentHash {
	return New(20)
}

func New(virutalMultis int) ConsistentHash {
	return &consistentHash{
		nodes:         make(map[string]bool),
		point2node:    make(map[uint32]string),
		virtualMultis: virutalMultis,
	}
}

func (c *consistentHash) Exist(node string) bool {
	c.RLock()
	defer c.RUnlock()
	return c.exist(node)
}

func (c *consistentHash) Add(node string) error {
	c.Lock()
	defer c.Unlock()

	if c.exist(node) {
		return ErrNodeAlreadyExist
	}

	c.nodes[node] = true
	for i := 0; i < c.virtualMultis; i++ {
		point := c.hashKey(c.virtualPoint(node, i))
		c.point2node[point] = node
	}
	c.update()
	return nil
}

func (c *consistentHash) Remove(node string) error {
	c.Lock()
	defer c.Unlock()

	if !c.exist(node) {
		return ErrNodeNotFound
	}

	delete(c.nodes, node)
	for i := 0; i < c.virtualMultis; i++ {
		point := c.hashKey(c.virtualPoint(node, i))
		delete(c.point2node, point)
	}
	c.update()
	return nil
}

func (c *consistentHash) Get(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.points) == 0 {
		return "", ErrNoNode
	}
	point := c.hashKey(key)
	return c.point2node[c.search(point)], nil
}

func (c *consistentHash) Nodes() []string {
	c.RLock()
	defer c.RUnlock()
	var ret []string
	for k, _ := range c.nodes {
		ret = append(ret, k)
	}
	return ret
}

func (c *consistentHash) Clear() {
	c.Lock()
	defer c.Unlock()
	c.nodes = nil
	c.points = nil
	c.point2node = nil
}

func (c *consistentHash) update() {
	c.points = nil
	for k, _ := range c.point2node {
		c.points = append(c.points, k)
	}
	sort.Sort(c.points)
}

func (c *consistentHash) virtualPoint(node string, index int) string {
	return fmt.Sprintf("%s-%d", node, index)
}

func (c *consistentHash) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *consistentHash) search(point uint32) uint32 {
	index := sort.Search(len(c.points), func(i int) bool {
		return c.points[i] >= point
	})
	if index < len(c.points) {
		return c.points[index]
	}
	return c.points[0]
}

func (c *consistentHash) exist(node string) bool {
	_, exist := c.nodes[node]
	return exist
}
