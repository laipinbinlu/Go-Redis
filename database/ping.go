package database

import (
	"go_redis/interface/resp"
	"go_redis/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply { // ping 指令  回复pong
	return reply.MakePongReply()
}

// init() 函数会在调用该包时候，执行该函数（初始化函数）
func init() {
	RegisterCommand("ping", Ping, 1) // 注册ping指令所对应方法
}
