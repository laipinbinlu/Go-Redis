package database

import "strings"

var cmdTable = make(map[string]*command) // 可以理解为指令对应的方法池

type command struct {
	exector ExecFunc // 指令对应的redis方法
	arity   int      //参数个数
}

func RegisterCommand(name string, exector ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		exector: exector,
		arity:   arity,
	}
}
