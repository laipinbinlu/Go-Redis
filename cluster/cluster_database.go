package cluster

import (
	"context"
	"go_redis/config"
	database2 "go_redis/database"
	"go_redis/interface/database"
	"go_redis/interface/resp"
	"go_redis/lib/consistenthash"
	"go_redis/lib/logger"
	"go_redis/resp/reply"
	"strings"

	pool "github.com/jolestar/go-commons-pool/v2"
)

type ClusterDatabase struct {
	self string

	nodes          []string                    // 所有的节点
	peerPicker     *consistenthash.NodeMap     // 节点选择器
	peerConnection map[string]*pool.ObjectPool // 每个节点的 客户端对象池
	db             database.Database
}

func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             database2.NewStandaloneDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}

	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	// 将所有节点的ip和端口号放入nodes中
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, cluster.self)
	cluster.nodes = nodes
	cluster.peerPicker.AddNode(nodes...)

	// 初始化连接池
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		p := pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
		cluster.peerConnection[peer] = p
	}

	return cluster
}

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply

var router = makeRouter() // 方法表

// 实现database接口
func (cluster *ClusterDatabase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(args[0]))
	cmdfunc, ok := router[cmdName]
	if !ok {
		reply.MakeErrReply("not supported cmd")

	}
	result = cmdfunc(cluster, client, args)

	return
}

func (cluster *ClusterDatabase) Close() {
	//
	cluster.db.Close()
}

func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}
