package cluster

import (
	"context"
	"errors"
	"go_redis/resp/client"

	pool "github.com/jolestar/go-commons-pool/v2"
)

type connectionFactory struct {
	Peer string
}

// 实现对应的接口

func (f *connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()
	return pool.NewPooledObject(c), nil
}

func (f *connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(client.Client)
	if !ok {
		return errors.New("type missmatch")
	}
	c.Close()
	return nil
}

func (f *connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (f *connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (f *connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}
