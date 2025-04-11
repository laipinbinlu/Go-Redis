package handler

import (
	"context"
	"go_redis/cluster"
	"go_redis/config"
	"go_redis/database"
	databaseface "go_redis/interface/database"
	"go_redis/lib/logger"
	"go_redis/lib/sync/atomic"
	"go_redis/resp/connection"
	"go_redis/resp/parser"
	"go_redis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var unknownErrBytes = []byte("-Err unknown\r\n")

type RespHandler struct { // 存在并发的问题,业务数据结构
	activeConn sync.Map //用来存储已经连接的客户的连接
	db         databaseface.Database
	closing    atomic.Boolean // 标记当前服务器是否（正在）关闭  ，防止后续用户进行连接
}

// new 一个hander
func MakeHandler() *RespHandler { // 其实就是确定数据的底层结构是什么
	var db databaseface.Database
	// 判断是否为集群redis
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 { //存在怕Peers 属于集群模式
		db = cluster.MakeClusterDatabase()
	} else { // 单机redis
		db = database.NewStandaloneDatabase()
	}

	return &RespHandler{
		db: db,
	}
}

// 关闭一个客户端
func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	r.db.AfterClientClose(client)
	r.activeConn.Delete(client) // 将map中记录的用户连接移除
}

// 实现hander接口

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Get() { // 如果当前的业务已经或者正在关闭，则直接关闭连接的用户
		conn.Close()
	}
	client := connection.NewConn(conn) // 包装好的用户
	r.activeConn.Store(client, struct{}{})
	ch := parser.ParseStream(conn) // 解析数据，resp协议
	// 获取ch中的数据  ->redis执行命令是单线程的
	for payload := range ch {
		// error  错误情况
		if payload.Err != nil {
			// 客户端关闭连接，或者网络连接关闭，那么就主动断开该客户端的连接
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of network connection") {
				r.closeClient(client)
				logger.Info("connection closed" + client.RemoteAddr().String())
				return
			}
			// 剩下的就是protocol error  协议错误
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes()) // 将错误信息返回给客户端
			if err != nil {                         // 如果回写出错，那么直接关闭
				r.closeClient(client)
				logger.Info("connection closed" + client.RemoteAddr().String())
				return
			}
			continue // 处理下一个请求结果。
		}
		// exec 正常的情况
		if payload.Data == nil { // 数据为空，没有必要执行
			continue
		}

		re, ok := payload.Data.(*reply.MultiBulkReply) // 判断是否解析的结果满足redis命令格式的要求 [][]byte 存储命令

		if !ok { // 解析失败，直接跳过
			logger.Error("require multi bulk reply")
			continue
		}

		// redis 业务执行数据
		result := r.db.Exec(client, re.Args)
		if result != nil {
			client.Write(result.ToBytes()) // 将redis处理之后的结果返回给客户端
		} else {
			client.Write(unknownErrBytes) // 如果还是出错，那就只能是未知错误
		}
	}
}

func (r *RespHandler) Close() error { // 关闭整个redis
	logger.Info("hander shuttuing down")
	r.closing.Set(true) // 将redis状态设置为true
	r.activeConn.Range( // 将还在连接状态的用户关闭
		func(key, value any) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true
		})
	r.db.Close()
	return nil
}
