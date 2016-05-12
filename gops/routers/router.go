package routers

import (
	"bfs/gops/controllers"
	"github.com/astaxie/beego"
)

func init() {

	beego.Router("/", &controllers.MainController{})
	beego.Router("/rack", &controllers.ApiController{},"get:GetRack")
	beego.Router("/group", &controllers.ApiController{},"get:GetGroup")
	beego.Router("/group", &controllers.ApiController{},"post:AddGroup")
	beego.Router("/freeStore", &controllers.ApiController{},"get:GetFreeStore")
	beego.Router("/addFreeVolume", &controllers.ApiController{},"post:AddFreeVolume")
	beego.Router("//addVolume", &controllers.ApiController{},"post:AddVolume")
}
