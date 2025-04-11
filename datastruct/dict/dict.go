package dict

type Consumer func(key string, val interface{}) bool

// redis 字典的核心业务   --- 关于string
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExits(key string, val interface{}) (result int)
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string // 随机返回多少个keys
	RandomDistinctKeys(limit int) []string
	Clear()
}
