package aof

import (
	"go_redis/config"
	"go_redis/interface/database"
	"go_redis/lib/logger"
	"go_redis/lib/utils"
	"go_redis/resp/connection"
	"go_redis/resp/parser"
	"go_redis/resp/reply"
	"io"
	"os"
	"strconv"
)

// aof append only file  将redis的写操作写入的文件中（持久化），实现redis的持久化。

const aofBufferSize = 1 << 16

type CmdLine = [][]byte

type payload struct { // 操作所对应的数据结构（封装）
	cmd     CmdLine
	dbIndex int
}

type AofHandler struct {
	database    database.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

// NewAofHandler
func NewAofHandler(database database.Database) (*AofHandler, error) {
	// 初始化
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename // aof存储所在的文件名
	handler.database = database
	// 加载原始的aof文件
	handler.LoadAof()

	aoffile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aoffile
	// channel的实现
	handler.aofChan = make(chan *payload, aofBufferSize)

	go func() { // 开启后台协程来进行aof持久化
		handler.handleAof()
	}()

	return handler, nil
}

// 向channel中写入操作的命令，直接将操作进行封装即可
func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil { // 前提是开启了aof并且chan存在
		handler.aofChan <- &payload{
			cmd:     cmd,
			dbIndex: dbIndex,
		}
	}
}

// handleAof   将chan 取得的操作进行落盘   -->操作持久化到文件中 ，协程下一直工作
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan { // 不断地从chan中取出操作
		if p.dbIndex != handler.currentDB { // 发生了数据库选择的变化，那么就必须落盘select 语句操作
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data) //  写入文件中
			if err != nil {                       // 发生错误，直接忽视
				logger.Error(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}

		// 数据库不改变，或者数据库改变之后 ---------->正常的操作落盘, 其实就是将用户的resp命令直接写入到文件中
		data := reply.MakeMultiBulkReply(p.cmd).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil { // 发生错误，直接忽视
			logger.Error(err)
		}

	}
}

// LoadAof 加载原先存在的aof文件  --- Aof 的恢复    前提 --> aof文件的指令都是按照resp协议 写的
func (handler *AofHandler) LoadAof() {
	file, err := os.Open(handler.aofFilename) // 已只读的方式打开文件  (1次)
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()
	// 关键思想就是aof文件 就是看成用户的指令解析重新执行一遍即可
	ch := parser.ParseStream(file)
	fackConn := &connection.Connection{} // 默认db为0
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF { // 文件读取完毕--退出
				break
			}
			logger.Error(p.Err)
			continue
		}
		// 没有错误---校验其他情况
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("aof file not multibulk")
			continue
		}
		// 拿到了数据 --> 执行命令
		re := handler.database.Exec(fackConn, r.Args)
		if reply.IsErrReply(re) {
			logger.Error("exec err " + string(re.ToBytes()))
		}

	}

}
