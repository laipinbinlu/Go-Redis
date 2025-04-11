package cluster

import (
	"context"
	"errors"
	"go_redis/interface/resp"
	"go_redis/lib/utils"
	"go_redis/resp/client"
	"go_redis/resp/reply"
	"strconv"
)

// 通信
// 从其他redis中对象池中获取一个用户的连接
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection not found")
	}
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	// 断言
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("wrong type")
	}
	return c, nil
}

// 使用完成后，要将连接释放归还给连接池
func (cluster *ClusterDatabase) returnPeerClient(peer string, c *client.Client) error {
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection not found")
	}
	return pool.ReturnObject(context.Background(), c)
}

// 命令的三种模式：单机，转发，群发   --->转发和群发需要另外实现
// 与进行用户的连接， 将命令转发给peer执行  -->将执行的结果返回
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self { // 如果是自身执行，那就直接执行
		return cluster.db.Exec(c, args)
	}
	// 其他redis执行
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	// 注意其他peer节点的数据库的选择,先选择数据库再进行操作
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

// 指令的群发(广播)     返回值为一组的reply
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	results := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		result := cluster.relay(node, c, args)
		results[node] = result
	}
	return results
}
