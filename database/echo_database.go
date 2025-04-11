package database

import (
	"go_redis/interface/resp"
	"go_redis/resp/reply"
)

// 实现回复用户内容的dabase业务
type EchoDatabase struct{}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e *EchoDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	return reply.MakeMultiBulkReply(args)
}

func (e *EchoDatabase) Close() {

}

func (e *EchoDatabase) AfterClientClose(c resp.Connection) {

}
