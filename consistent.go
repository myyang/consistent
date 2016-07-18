// Package consistent provides ...
package consistent

import (
	"fmt"
	"hash/crc64"
	"hash/fnv"
	"sort"
	"sync"
)

// Default constants
const (
	DefaultReplica = 100
	CRC64ECMA128   = 0xC96C5795D7870F42
)

// default variables
var (
	CRC64ECMA128Table = crc64.MakeTable(CRC64ECMA128)
)

// NewConsistent return new consistent with default value, replica number: 100 and hash algo: crc64
func NewConsistent() *Consistent {
	return NewConsistentWithN(DefaultReplica)
}

// NewConsistentWithN return consistent with given replica number and defautl hash algo: crc64
func NewConsistentWithN(replicas int) *Consistent {
	return NewConsistentWithHash(replicas, crc64h)
}

// NewConsistentWithHash return consistent with given hash algorithm
func NewConsistentWithHash(replicas int, fn HashFunc) *Consistent {
	c := &Consistent{}
	c.node = make(map[string]bool)
	c.nodesmap = make(map[uint64]string)
	c.setReplica(replicas)
	c.setHashFunc(fn)
	return c
}

// HashFunc provides flexibility to give desired hash algorithm
type HashFunc func([]byte) uint64

func fnvh(key []byte) uint64 {
	// not balanced while compute
	// pending for verifying
	cryptor := fnv.New64a()
	cryptor.Write(key)
	return cryptor.Sum64()
}

func crc64h(key []byte) uint64 {
	return crc64.Checksum(key, CRC64ECMA128Table)
}

type suint64 []uint64

func (s suint64) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s suint64) Len() int           { return len(s) }
func (s suint64) Less(i, j int) bool { return s[i] < s[j] }

type consistentError struct {
	Msg string
}

func (c consistentError) Error() string {
	return fmt.Sprintf("Consistent Error, %v\n", c.Msg)
}

// Consistent struct
type Consistent struct {
	mu       sync.RWMutex
	count    int
	node     map[string]bool
	nodesmap map[uint64]string
	nodeskey suint64
	replicas int
	hashfunc HashFunc
}

func (c *Consistent) setReplica(n int) {
	// at least one node, hide this from client
	if n <= 0 {
		n = 1
	}
	c.replicas = n
}

func (c *Consistent) setHashFunc(fn HashFunc) {
	c.hashfunc = fn
}

func (c *Consistent) hashKey(key []byte, i int) uint64 {
	for i > 0 {
		j := byte(i % 256)
		i /= 256
		key = append(key, j)
	}
	return c.hashfunc(key)
}

// AddNode to consistent
func (c *Consistent) AddNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.node[node]; ok {
		return
	}
	nodeByte := []byte(node)
	for i := 0; i < c.replicas; i++ {
		key := c.hashKey(nodeByte, i)
		c.nodesmap[key] = node
		c.nodeskey = append(c.nodeskey, key)
	}
	sort.Sort(c.nodeskey)
	c.node[node] = true
	c.count++
}

// AddNodes provides shortcut to add multiple nodes
func (c *Consistent) AddNodes(nodes []string) {
	for _, n := range nodes {
		c.AddNode(n)
	}
}

// RemoveNode from consistent
func (c *Consistent) RemoveNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.node[node]; !ok {
		return
	}
	nodeByte := []byte(node)
	for i := 0; i < c.replicas; i++ {
		key := c.hashKey(nodeByte, i)
		delete(c.nodesmap, key)
		c.remove(key)
	}
	delete(c.node, node)
	c.count--
}

// RemoveNodes provides shortcut to remove nodes
func (c *Consistent) RemoveNodes(nodes []string) {
	for _, n := range nodes {
		c.RemoveNode(n)
	}
}

func (c *Consistent) remove(key uint64) {
	i := c.search(key)
	c.nodeskey = append(c.nodeskey[:i], c.nodeskey[i+1:]...)
}

// GetNode returns first found node
func (c *Consistent) GetNode(key string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.nodeskey) == 0 {
		return "", consistentError{Msg: "Empty! No nodes."}
	}
	ind := c.searchKey(key)
	node := c.getNode(ind)
	return node, nil
}

func (c *Consistent) search(key uint64) int {
	ind := sort.Search(len(c.nodeskey), func(i int) bool { return c.nodeskey[i] >= key })
	if ind >= len(c.nodeskey) {
		ind = 0
	}
	return ind
}

func (c *Consistent) searchKey(key string) int {
	return c.search(c.hashfunc([]byte(key)))
}

// GetNNode returns found distinct nodes with given n
func (c *Consistent) GetNNode(key string, n int) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if n > c.count {
		return []string{}, consistentError{Msg: "Query N is greater than total nodes"}
	}
	var nodes []string
	ind, max := c.searchKey(key), c.replicas*c.count-1
	for len(nodes) < n {
		if t := c.getNode(ind); !stringInSlice(nodes, t) {
			nodes = append(nodes, t)
		}
		if ind < max {
			ind++
		} else {
			ind = 0
		}
	}
	return nodes, nil
}

func stringInSlice(l []string, x string) bool {
	for _, s := range l {
		if s == x {
			return true
		}
	}
	return false
}

func (c *Consistent) getNode(ind int) string {
	return c.nodesmap[c.nodeskey[ind]]
}

// Get3Node is shortcut to get 3 Node
// Becasue 3 replica/sharding is a practical number for performance and robust
func (c *Consistent) Get3Node(key string) ([]string, error) {
	return c.GetNNode(key, 3)
}

// HasNode tests exsiting node
func (c *Consistent) HasNode(node string) bool {
	_, ok := c.node[node]
	return ok
}

// NodeNumber return currently physical node number
func (c *Consistent) NodeNumber() int {
	return c.count
}
