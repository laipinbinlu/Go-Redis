package tcp

import (
	"bufio"
	"context"
	"go_redis/lib/logger"
	"go_redis/lib/sync/atomic"
	"go_redis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

type EchoClient struct { // echoclient时的客户端
	Conn    net.Conn
	Waiting wait.Wait
}

func (e *EchoClient) Close() error { // 关闭功能
	e.Waiting.WaitWithTimeout(10 * time.Second) // 定时等待
	e.Conn.Close()
	return nil
}

type EchoHandler struct { // 存在并发的问题,业务数据结构
	activeConn sync.Map
	closing    atomic.Boolean
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

func (e *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if e.closing.Get() { // 如果当前的业务已经或者正在关闭，则直接关闭连接的用户
		conn.Close()
	}
	// 没有，处理请求
	client := &EchoClient{ // 封装客户端的连接
		Conn: conn,
	}
	e.activeConn.Store(client, struct{}{}) // 将封装好的用户存储到map中,记录当前用户的连接
	reader := bufio.NewReader(conn)
	//读取用户传递的消息
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("Connecting close") // 代表用户发完来数据，或者是用户退出连接诶
				e.activeConn.Delete(client)     // 删除该用户
			} else {
				logger.Warn(err)
			}
			return
		}
		// 处理数据
		client.Waiting.Add(1)
		b := []byte(msg)
		client.Conn.Write(b) // 回写数据
		client.Waiting.Done()
	}
}

func (e *EchoHandler) Close() error { // 业务关闭操作
	logger.Info("handler shutting down")
	e.closing.Set(true)                            // 先设置为关闭
	e.activeConn.Range(func(key, value any) bool { // 关闭连接服务的所有连接
		client := key.(*EchoClient)
		client.Conn.Close()
		return true // 传递函数来range执行，true会一直执行，false则会终止
	})
	return nil
}
