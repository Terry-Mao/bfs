package main

import (
	_ "bfs/gops/routers"
	"github.com/astaxie/beego"
	"bfs/gops/models/ops"
)

func main() {
	ops.InitOps()
	beego.Run()
}

