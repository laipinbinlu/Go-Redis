package database

import (
	"go_redis/aof"
	"go_redis/config"
	"go_redis/interface/resp"
	"go_redis/lib/logger"
	"go_redis/resp/reply"
	"strconv"
	"strings"
)

// 真正的database 内核业务
type StandaloneDatabase struct {
	dbSet      []*DB           // 多个redis数据库组成
	aofHandler *aof.AofHandler //aof持久化技术
}

// 初始化 database
func NewStandaloneDatabase() *StandaloneDatabase {
	//主要是根据初始化文件来设置database
	database := &StandaloneDatabase{}
	if config.Properties.Databases == 0 { // 没有指定参数使用默认参数16
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*DB, config.Properties.Databases) // redis默认16个数据库
	// 设置DB的值, 为切片的每一个db设置值
	for i := range database.dbSet {
		db := makeDB()
		db.index = i
		database.dbSet[i] = db
	}
	// 初始化aofhandler   // aof机制
	if config.Properties.AppendOnly { // 是否启动了aof
		aofHandler, err := aof.NewAofHandler(database)
		if err != nil {
			panic(err) // redis业务还没有启动，可以panic
		}
		database.aofHandler = aofHandler
		// 给每一个db初始化addaof方法   ---- 注意闭包问题
		for _, db := range database.dbSet {
			ldb := db
			ldb.addAof = func(line CmdLine) {
				database.aofHandler.AddAof(ldb.index, line) // 函数内部的变量引用函数外部的变量会引发闭包问题
			}
		}

	}

	return database
}

// 实现database接口服务
// 执行业务函数 --- 主要逻辑就是将exec方法交给具体的db执行 传入的命令都是[][]byte格式
// 命令 ->  GET 2   SET K V   SELECT 2
func (e *StandaloneDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {

	defer func() { // redis核心业务，使用recover防止panci导致系统崩溃
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	// 需要单独处理select命令,底层db是没有实现select处理的
	cmdName := strings.ToLower(string(args[0]))
	if cmdName == "select" { // 是的话
		if len(args) != 2 { // 校验
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(client, e, args[1:]) // 处理selct指令
	}
	// 一般的语句--- 发配给具体的db, db的index就是记录在封装的用户的结构体中
	dbindex := client.GetDBIndex()
	db := e.dbSet[dbindex]
	return db.Exec(client, args)

}

func (e *StandaloneDatabase) Close() {

}

func (e *StandaloneDatabase) AfterClientClose(c resp.Connection) {

}

// 执行用户选择数据库的指令
// select 2
func execSelect(c resp.Connection, database *StandaloneDatabase, args [][]byte) resp.Reply {
	dbindex, err := strconv.Atoi(string(args[0]))
	if err != nil { // 出现了 select a/b  ..
		return reply.MakeErrReply("Err invalid DB index")
	}
	// select 90000
	if dbindex >= len(database.dbSet) {
		return reply.MakeErrReply("Err DB index out of range")
	}
	// 正常情况  --- 选择数据库
	c.SelectDB(dbindex)
	return reply.MakeOkReply()
}
