package connection

import (
	"go_redis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// redis 协议对连接上的客户端的描述-->对连接的conn进行包装
type Connection struct {
	conn         net.Conn
	waitingReply wait.Wait // 用于等待所有待发送的响应数据发送完成后再关闭连接
	mu           sync.Mutex
	selectedDB   int
}

// 对用户连接进行包装
func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

func (c *Connection) RemoteAddr() net.Addr { // 返回客户端的ip地址
	return c.conn.RemoteAddr()
}

func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second) //关闭前先等待其他协程执行完毕，定时时长为10s，等待redis处理完对应用户的请求
	c.conn.Close()
	return nil
}

// 实现Connection 接口
func (c *Connection) Write(bytes []byte) error { // redis 给连接的客户写入数据方法
	if len(bytes) == 0 { // 数据长度为0，那么就不写入
		return nil
	}

	c.mu.Lock() // 防止redis并发写入数据给用户
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(bytes) // redis 发送数据给连接的客户  ，将 bytes 发送到 conn，而 conn 代表的是 客户端的 TCP 连接。
	return err
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(a int) {
	c.selectedDB = a
}

/*
Redis 服务器是多线程的
在 Redis 服务器中，每个客户端的请求可能由多个 goroutine 处理：
一个 goroutine 负责解析命令
另一个 goroutine 负责执行命令   --->redis执行命令是单线程的
可能还有其他 goroutine 负责日志记录、超时检测等
如果多个 goroutine 同时调用 Write 方法，而 net.Conn.Write(bytes) 不是并发安全的，就可能出现数据交错，比如：
Goroutine A 写入 "OK\r\n"
Goroutine B 写入 ":100\r\n"
如果没有加锁，客户端可能会收到 "O:100K\r\n"，导致协议错误。
*/
