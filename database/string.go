package database

import (
	"go_redis/interface/database"
	"go_redis/interface/resp"
	"go_redis/lib/utils"
	"go_redis/resp/reply"
)

//string类型 的常用指令

// GET k1
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	v, ok := db.GetEntity(key)
	if !ok { //k 不存在
		return reply.MakeNullBulkReply()
	}
	bytes, _ := v.Data.([]byte) // 进行类型断言，只是实现了string，所以一定可以断言成功，不需要进一步判断
	return reply.MakeBulkReply(bytes)
}

// SET K V
func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	// 就是将value包装一下即可
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine3("set", args...))
	return reply.MakeOkReply()
}

// SETNX K1 V1   k1存在则返回0 不操作；不存在 1 则插入
func execSetnx(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	// 就是将value包装一下即可
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine3("setnx", args...))
	return reply.MakeIntReply(int64(result))
}

// GETSET K1 V1   先获取k1原先的v，再将k1的值设置为新的v1
func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	oldvalue, ok := db.GetEntity(key)
	db.PutEntity(key, &database.DataEntity{
		Data: value,
	})
	db.addAof(utils.ToCmdLine3("getset", args...))
	if !ok { //原先key不存在
		return reply.MakeNullBulkReply()
	}
	//存在，返回原来的值，包装为resp协议的格式
	result, _ := oldvalue.Data.([]byte)
	return reply.MakeBulkReply(result)
}

// STRLEN k -> 'value'   返回key指向的值的字符串的长度
func execStrlen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value, ok := db.GetEntity(key)
	if !ok {
		return reply.MakeNullBulkReply()
	}
	result, _ := value.Data.([]byte)
	return reply.MakeIntReply(int64(len(result)))
}

func init() {
	RegisterCommand("get", execGet, 2)
	RegisterCommand("set", execSet, 3)
	RegisterCommand("setnx", execSetnx, 3)
	RegisterCommand("getset", execGetSet, 3)
	RegisterCommand("strlen", execStrlen, 2)
}
