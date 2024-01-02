package utils

import (
	"fmt"
	"runtime"
)

func Assert(result bool, message string) {
	if !result {
		// 获取 panic 发生的文件名和行号
		_, file, line, _ := runtime.Caller(1)
		panic(fmt.Errorf("Panic occurred at %s:%d for error:%s \n", file, line, message))
	}
}
