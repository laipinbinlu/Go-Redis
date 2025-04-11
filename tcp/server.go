package tcp

import (
	"context"
	"go_redis/interface/tcp"
	"go_redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

func ListenAndServeWithSignal(cfg *Config, hander tcp.Handler) error {

	closeChan := make(chan struct{})
	signChan := make(chan os.Signal)
	signal.Notify(signChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT) // 系统的指令 -- 挂起，退出，终止、中断
	go func() {                                                                               // 注意服务段的关闭信号，使用chan来同步关闭信号的操作
		sig := <-signChan // 系统传递的关闭连接的信号
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	l, err := net.Listen("tcp", cfg.Address) // tcp连接所设置的地址
	if err != nil {
		return err
	}
	logger.Info("start listen")
	// 将l传递到下面的函数进行监听
	ListenAndServe(l, hander, closeChan)
	return nil

}

func ListenAndServe(listener net.Listener, hander tcp.Handler, closeChan <-chan struct{}) {

	go func() { // 监听系统传递的关闭信号
		<-closeChan
		logger.Info("shutting down")
		listener.Close()
		hander.Close()
	}()

	defer func() {
		listener.Close()
		hander.Close() // 业务结束时，要关闭所有的连接
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup // 并发的waitGroup
	for {
		c, err := listener.Accept()
		if err != nil { //接受新的连接出现错误，那么tcp直接取消监听。
			break
		}
		logger.Info("accept link")
		waitDone.Add(1)
		go func() { // 使用协程来异步地处理请求
			defer waitDone.Done()
			hander.Handle(ctx, c)
		}()
	}
	waitDone.Wait() // 出现了错误退出时，需要等待其协程的连接执行完毕之后，才能终止
}
