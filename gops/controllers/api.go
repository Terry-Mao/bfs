package controllers

import (
	"github.com/astaxie/beego"
	"bfs/gops/models/ops"
	"strings"
)

type ApiController struct {
	beego.Controller
}

func (c *ApiController) Initialization() {

}

func (c *ApiController) GetRack() {
	var err error
	if c.Data["json"], err = ops.OpsManager.GetRack(); err != nil {
		c.Abort("500")
		beego.Error(err)
	}

	c.ServeJSON()
}


func (c *ApiController) GetGroup() {
	var err error
	if c.Data["json"], err = ops.OpsManager.GetGroup(); err != nil {
		beego.Error(err)
		c.Abort("500")
	}

	c.ServeJSON()
}



func (c *ApiController) GetFreeStore() {
	var err error
	if c.Data["json"], err = ops.OpsManager.GetFreeStore(); err != nil {
		beego.Error(err)
		c.Abort("500")
	}

	c.ServeJSON()
}

func (c *ApiController)AddFreeVolume() {

	var (
		err error
		host string
		bdir string
		idir string
		n int32
		res map[string]interface{}
	)

	res = make(map[string]interface{})
	res["success"] = true

	host = c.GetString("host")
	bdir = c.GetString("bdir")
	idir = c.GetString("idir")
	if n, err = c.GetInt32("n"); err !=nil{
		beego.Error(err)
		c.responseError(err)
	}

	if err = ops.OpsManager.AddFreeVolume(host,n,bdir,idir);err != nil{
		beego.Error(err)
		c.responseError(err)
	}

	c.Data["json"] = res
	c.ServeJSON()
}

func (c *ApiController)AddGroup() {
	var (
		err error
		stores []string
		racks int
		copys int
		res map[string]interface{} = make(map[string]interface{})
	)


	res["success"] = true

	stores = strings.Split(c.GetString("stores"), ",")

	if racks, err = c.GetInt("racks"); err != nil {
		beego.Error(err)
		c.responseError(err)
	}

	if copys, err = c.GetInt("copys"); err != nil {
		beego.Error(err)
		c.responseError(err)
	}

	if err = ops.OpsManager.AddGroup(stores, racks, copys); err != nil {
		beego.Error(err)
		c.responseError(err)
	}

	c.Data["json"] = res
	c.ServeJSON()
}

func (c *ApiController)AddVolume() {
	var (
		err error
		groupId int64
		n int
		res map[string]interface{} = make(map[string]interface{})
	)


	res["success"] = true


	if groupId, err = c.GetInt64("groupId"); err != nil {
		beego.Error(err)
		c.responseError(err)
	}

	if n, err = c.GetInt("n"); err != nil {
		beego.Error(err)
		c.responseError(err)
	}

	if err = ops.OpsManager.AddVolume(uint64(groupId),n); err != nil {
		beego.Error(err)
		c.responseError(err)
	}

	c.Data["json"] = res
	c.ServeJSON()
}




func (c *ApiController)responseError(err error)  {
	res := make(map[string]interface{})
	res["success"] = false
	res["msg"] = err.Error()
	c.Data["json"] = res
	c.ServeJSON()
}