package main

import (
	"fmt"
	"go_redis/config"
	"go_redis/lib/logger"
	"go_redis/resp/handler"
	"go_redis/tcp"
	"os"
)

const configFile string = "redis.conf"

var defaultProperties = &config.ServerProperties{ // 默认配置
	Bind: "0.0.0.0",
	Port: 6379,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	logger.Setup(&logger.Settings{ // 日志的默认格式
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	if fileExists(configFile) { // 判断是否存在配置文件
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}

	// 调用tcp连接服务，监听配置文件中对应的端口号
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, handler.MakeHandler()) // 传入处理的函数即服务给对应的连接
	if err != nil {
		logger.Error(err)
	}

}
