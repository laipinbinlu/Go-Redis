package database

import (
	"go_redis/interface/resp"
	"go_redis/lib/utils"
	"go_redis/lib/wildcard"
	"go_redis/resp/reply"
)

// 实现keys 指令操作（方法）

// Del k1 k2 k3...
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	} // 先转化为string类型，在交给下层的db函数执行
	deleted := db.Removes(keys...)
	if deleted > 0 { // 如果删除，那么aof记录
		db.addAof(utils.ToCmdLine3("del", args...))
	}
	return reply.MakeIntReply(int64(deleted)) // 得到执行结果，包装为resp协议的格式返回为用户
}

// EXISTS K1 K2 K3 ...
func execExists(db *DB, args [][]byte) resp.Reply {
	// 返回存在的个数
	result := 0
	for _, v := range args {
		key := string(v)
		_, ok := db.GetEntity(key)
		if ok { // 如果存在，那么结果数量++
			result++
		}
	}
	return reply.MakeIntReply(int64(result))
}

// FLUSHDB
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	// 清空数据库
	db.Flush()
	db.addAof(utils.ToCmdLine3("flushdb", args...))
	return reply.MakeOkReply()
}

// TYPE k1 返回key的值的类型
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none") // :none\r\n   不存在返回none
	}
	switch entity.Data.(type) { // 类型断言
	case []byte:
		return reply.MakeStatusReply("string")
	}
	return &reply.UnknownErrReply{}
}

// Rename k1 k2 k1:v -> k2:v        将key重新命名为另一个key，实际就是先删除，在创建
func execRename(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dst := string(args[1])
	v, ok := db.GetEntity(src)
	if !ok { // 不存在的key
		return reply.MakeErrReply("on such key")
	}
	db.PutEntity(dst, v)
	db.Remove(src)
	db.addAof(utils.ToCmdLine3("rename", args...))
	return reply.MakeOkReply()
}

// RENAMENX  K1 K2 , 主要区别就是检查一下是否k2已经存在
func execRenamenx(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dst := string(args[1])
	_, ok := db.GetEntity(dst)
	if ok {
		return reply.MakeIntReply(0) // 存在，返回0表示操作失败
	}
	v, ok := db.GetEntity(src)
	if !ok { // 不存在的key
		return reply.MakeErrReply("on such key")
	}
	db.PutEntity(dst, v)
	db.Remove(src)
	db.addAof(utils.ToCmdLine3("renamenx", args...))
	return reply.MakeIntReply(1) // k2不存在，返回1表示操作成功
}

// KEYS *  返回所以满足要求的keys  （通配符要求）
func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern, _ := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)
	db.Data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

// init 函数
func init() {
	RegisterCommand("del", execDel, -2)
	RegisterCommand("exists", execExists, -2)
	RegisterCommand("flushdb", execFlushDB, -1) // FLUSHDB a ,b ,c  只需要执行flush命令，不管参数的长度。所以取-1
	RegisterCommand("type", execType, 2)        // TYPE K1
	RegisterCommand("rename", execRename, 3)    // RENAME K1 K2
	RegisterCommand("renamenx", execRenamenx, 3)
	RegisterCommand("keys", execKeys, 2) // KEYS  *    只是接受两个参数，keys 通配符
}
