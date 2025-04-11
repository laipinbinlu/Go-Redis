package dict

import (
	"sync"
)

type SyncDict struct {
	m sync.Map
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

// 实现Dict接口方法  --》实际就是调用sync.Map来实现该功能
func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m.Load(key)
	return val, ok
}

func (dict *SyncDict) Len() int {
	// 遍历syncmap 取得长度
	length := 0
	dict.m.Range(func(key, value any) bool {
		length++
		return true
	})
	return length
}

func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	// 写操作 -->分为插入还是修改
	_, existed := dict.m.Load(key)
	dict.m.Store(key, val)
	if existed {
		return 0 // 已经存在，那么返回0表示为修改操作
	}
	return 1 // 不存在，则就是真的插入操作
}

func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key) // 只有不存在才插入，否则不插入
	if existed {
		return 0 // 已经存在 插入失败
	}
	dict.m.Store(key, val)
	return 1 //   不存在 插入成功
}

func (dict *SyncDict) PutIfExits(key string, val interface{}) (result int) {
	// 存在才插入（修改） ，不存在就不插入
	_, existed := dict.m.Load(key)
	if existed {
		dict.m.Store(key, val)
		return 1 // 已经存在 插入成功
	}
	return 0 //   不存在 插入失败
}

func (dict *SyncDict) Remove(key string) (result int) {
	// 删除
	_, existed := dict.m.Load(key)
	dict.m.Delete(key)
	if existed {
		return 1 // 删除成功
	}
	return 0 //不存在该key，删除失败
}

func (dict *SyncDict) ForEach(consumer Consumer) { // 遍历
	dict.m.Range(func(key, value any) bool {
		consumer(key.(string), value)
		return true
	})
}

func (dict *SyncDict) Keys() []string {
	// 返回所有的keys
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value any) bool {
		result[i] = key.(string)
		i++
		return true
	})
	return result
}

func (dict *SyncDict) RandomKeys(limit int) []string {
	// 随机取出limit个key  可以重复
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		dict.m.Range(func(key, value any) bool {
			result[i] = key.(string)
			return false
		})
	}
	return result
}

func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, limit)
	i := 0
	dict.m.Range(func(key, value any) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return result
}

func (dict *SyncDict) Clear() {
	// 清空数据，直接换成一个新的map即可
	*dict = *MakeSyncDict()
}
