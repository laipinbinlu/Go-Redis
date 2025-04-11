package database

import (
	"go_redis/datastruct/dict"
	"go_redis/interface/database"
	"go_redis/interface/resp"
	"go_redis/resp/reply"
	"strings"
)

// redis 上层面向用户的数据结构db
type DB struct {
	index  int       // 当前数据库的编号
	Data   dict.Dict //对应的接口方法，底层的sync.Map结构体会实现该方法
	addAof func(CmdLine)
}

// redis的执行函数的格式
type ExecFunc func(db *DB, args [][]byte) resp.Reply

type CmdLine = [][]byte

func makeDB() *DB {
	return &DB{
		Data:   dict.MakeSyncDict(), // 返回实现该接口的sync.Map结构体指针
		addAof: func(cl CmdLine) {},
	}
}

// 执行指令    参数：连接，命令
func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	// PING  SET  SETNX....
	cmdName := strings.ToLower(string(cmdLine[0])) // 转化为小写   -->将首个命令取出，在对应的命令池中找到执行的函数
	cmd, ok := cmdTable[cmdName]                   // 从注册过的指令池中取出command 指令
	if !ok {
		return reply.MakeErrReply("Err unknown command" + cmdName) // 未知命令
	}
	// SET K
	if !validateArity(cmd.arity, cmdLine) { // 参数错误
		return reply.MakeArgNumErrReply(cmdName)
	}
	// 校验参数无误，执行相关的命令
	fun := cmd.exector // 真正的执行函数
	//Set k  v  -> k, v  只需要k  v即可
	return fun(db, cmdLine[1:])
}

// SET k v  -> arity 3
// EXISTS  k1 k2 k3 k4 ......   ->arity = -2  可变长参数
func validateArity(arity int, cmdArgs CmdLine) bool { // 校验参数是否正确
	argNum := len(cmdArgs)
	if arity >= 0 { // 如果是非变长参数，直接判断是否参数合理
		return argNum == arity
	}
	return argNum >= -arity // 可变长参数   至少需要一个命令和一个参数
}

// 在db层实现一些操作(套一层)，获取该元素
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	val, ok := db.Data.Get(key)
	if !ok { // 不存在
		return nil, false
	}
	return val.(*database.DataEntity), true
}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.Data.Put(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.Data.PutIfAbsent(key, entity)
}

func (db *DB) PutIfExits(key string, entity *database.DataEntity) int {
	return db.Data.PutIfExits(key, entity)
}

func (db *DB) Remove(key string) {
	db.Data.Remove(key)
}

func (db *DB) Removes(keys ...string) int { // 删除多个keys，返回删除的个数
	deleted := 0
	for _, key := range keys {
		_, ok := db.Data.Get(key)
		if ok {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	db.Data.Clear()
}
