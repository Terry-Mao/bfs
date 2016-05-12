<!DOCTYPE html>
<html ng-app="myapp">
<head>
    <title>BFS Console</title>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <link rel="stylesheet" href="//cdn.bootcss.com/bootstrap/3.3.5/css/bootstrap.min.css">
    <style>
        body {
            padding: 20px;
        }

        td {
            cursor: pointer;
        }

    </style>

    <script src="//cdn.bootcss.com/jquery/1.11.3/jquery.min.js"></script>
    <script src="//cdn.bootcss.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
    <script src="static/js/angular.js"></script>
</head>

<body>

<!-- Store初始化窗口-->
<div id="initFreeVolumeDialog" class="modal fade" tabindex="-1" role="dialog" aria-labelledby="myLargeModalLabel"
     ng-controller="initFreeVolume">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span
                        aria-hidden="true">&times;</span></button>
                <h4 class="modal-title">请选择至少2个Store...</h4>
            </div>
            <div class="modal-body">
                <form id="initFreeVolumeForm">
                    <input type="text" ng-model="formData.host" type="text" class="hide"/>
                    <fieldset>
                        <label>数据目录:</label>
                        <input name="bdir" ng-model="formData.bdir" type="text"/>
                    </fieldset>
                    <fieldset>
                        <label>索引目录:</label>
                        <input name="idir" ng-model="formData.idir" type="text"/>
                    </fieldset>
                    <fieldset>
                        <label>卷数量&nbsp;&nbsp;&nbsp;&nbsp;:</label>
                        <input name="n" ng-model="formData.n" type="text"/>
                    </fieldset>
                </form>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-default" data-dismiss="modal">取消</button>
                <button type="button" class="btn btn-primary" ng-click="processForm()">提交</button>
            </div>
        </div>
    </div>
</div>

<!-- 添加分组窗口-->
<div id="freeStore" class="modal fade" tabindex="-1" role="dialog" aria-labelledby="myLargeModalLabel"
     ng-controller="freeStore">
    <div class="modal-dialog modal-lg">

        <div class="modal-content">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span
                        aria-hidden="true">&times;</span></button>
                <h4 class="modal-title">请选择至少2个Store...</h4>
            </div>
            <div class="modal-body">
                <span ng-if="items.length == 0">没有可用的store...</span>
                <table class="table table-bordered" ng-if="items.length > 0">
                    <tr>
                        <th class="col-md-1"></th>
                        <th class="col-md-3">StoreId</th>
                        <th class="col-md-3">Ip</th>
                    </tr>

                    <tr ng-repeat="store in items">
                        <td><input type="checkbox" ng-model="flag" ng-click="toggleChecked(flag,store.id)"/></td>
                        <td>{{store.id}}</td>
                        <td>{{store.ip}}</td>
                    </tr>
                </table>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-default" data-dismiss="modal">取消</button>
                <button type="button" class="btn btn-primary" ng-click="addGroup()">添加</button>
            </div>
        </div>
    </div>
</div>

<!-- 扩容group窗口-->
<div id="addVolumeDialog" class="modal fade" tabindex="-1" role="dialog" aria-labelledby="myLargeModalLabel"
     ng-controller="addVolume">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span
                        aria-hidden="true">&times;</span></button>
                <h4 class="modal-title">添加卷</h4>
            </div>
            <div class="modal-body">
                <form id="addVolumeForm">
                    <input type="text" ng-model="formData.groupId" type="text" class="hide"/>
                    <fieldset>
                        <label>数量&nbsp;&nbsp;&nbsp;&nbsp;:</label>
                        <input name="n" ng-model="formData.n" type="text"/>
                    </fieldset>
                </form>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-default" data-dismiss="modal">取消</button>
                <button type="button" class="btn btn-primary" ng-click="processForm()">提交</button>
            </div>
        </div>
    </div>
</div>

<div class=container>
    <div class="row">
        <div class="col-md-10">
            <h4>机架信息</h4>
        </div>
    </div>
    <div class="row" ng-controller="rack">
        <div class="col-md-10" ng-repeat="rack in items">
            <div class="panel panel-default" ng-repeat="group in items">
                <div class=panel-heading><b>{{rack.name}}</b>
                </div>
                <div class=panel-body>
                    <table class="table table-bordered">
                        <tr>
                            <th class="col-md-4">Store</th>
                            <th class="col-md-4">Ip</th>
                            <th class="col-md-2">操作</th>
                        </tr>
                        <tr ng-repeat="store in rack.stores">
                            <td>{{store.id}}</td>
                            <td>{{store.ip}}</td>
                            <td>
                                <button class="btn btn-xs btn-primary" ng-click="initFreeVolume(store.admin)">
                                    初始化卷
                                </button>
                                <button class="btn btn-xs btn-primary" ng-click="storeInfo(store.stat)">状态</button>
                            </td>
                        </tr>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <div class=row>
        <div class=col-md-10>
            <h4>分组信息
                <button type="button" class="btn btn-sm btn-default" data-toggle="modal" data-target="#freeStore">
                    <span class="glyphicon glyphicon-plus"></span>添加分组
                </button>
            </h4>
        </div>
        <p>

        </p>
    </div>


    <div class="row" ng-controller="group">
        <div class="col-md-10">
            <div class="panel panel-default" ng-repeat="group in items">
                <div class=panel-heading><b>group_{{ group.id }}</b>

                    <div class="btn-group btn-group-sm pull-right">
                        <button class="btn btn-sm btn-default" ng-click="openDailog(group.id)"
                                title="Add Redis Server"><span class="glyphicon glyphicon-plus"></span>
                            添加卷
                        </button>
                    </div>
                </div>
                <div class=panel-body>
                    <table class="table table-bordered">
                        <tr>
                            <th class="col-md-4">Store</th>
                            <th class="col-md-4">Ip</th>
                            <th class="col-md-2">操作</th>
                        </tr>
                        <tr ng-repeat="store in group.stores">
                            <td>{{store.id}}</td>
                            <td>{{store.ip}}</td>
                            <td>
                                <button class="btn btn-xs btn-primary" ng-click="initFreeVolume(store.admin)">
                                    初始化卷
                                </button>
                                <button class="btn btn-xs btn-primary" ng-click="storeInfo(store.stat)">状态</button>
                            </td>
                        </tr>
                    </table>
                </div>
            </div>
        </div>
    </div>
</div>


</body>

<script src="static/js/index.js"></script>
</html>
