package database

import "go_redis/interface/resp"

type CmdLine = [][]byte

// redis的核心业务
type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply
	Close()
	AfterClientClose(c resp.Connection)
}

type DataEntity struct { //redis的数据类型
	Data interface{}
}
