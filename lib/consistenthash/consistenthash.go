package consistenthash

import (
	"hash/crc32"
	"sort"
)

// 一致性hash的实现

type HashFunc func(data []byte) uint32

type NodeMap struct {
	hashFunc    HashFunc       // hash 函数
	nodeHashs   []int          // 每个集群节点的hash值   12343 28989
	nodehashMap map[int]string // hash值所对应的节点的地址
}

func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil { // 判断hash函数是否为nil
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *NodeMap) IsEmpty() bool { // 节点集群是否为空
	return len(m.nodeHashs) == 0
}

// 增加集群节点到hash中，按照一致性hash的方式处理即可
func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key))) // 得到hash，之后存入数组中和map记录对应的关系
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodehashMap[hash] = key
	}
	// 排序
	sort.Ints(m.nodeHashs)
}

// 判断存入的k v 属于哪一个节点
func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}
	hash := int(m.hashFunc([]byte(key))) // 拿到key对应的hash
	// 在节点hash中寻找大的，那么该key属于该节点，特殊情况就是第一个节点
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})
	if idx == len(m.nodeHashs) {
		idx = 0
	}
	// 返回节点的地址（名称）
	return m.nodehashMap[m.nodeHashs[idx]]
}
