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
                <h4 class="modal-title">Init Volume[{{storeId}}]</h4>
            </div>
            <div class="modal-body">
                <form id="initFreeVolumeForm">
                    <input type="text" ng-model="formData.host" type="text" class="hide"/>

                    <div class="form-group">
                        <label class="col-sm-2 control-label">Data Dir:</label>
                        <input name="bdir" ng-model="formData.bdir" type="text"/>
                    </div>
                    <div class="form-group">
                        <label class="col-sm-2 control-label">Index Dir:</label>
                        <input name="idir" ng-model="formData.idir" type="text"/>
                    </div>
                    <div class="form-group">
                        <label class="col-sm-2 control-label">Num&nbsp;&nbsp;&nbsp;&nbsp;:</label>
                        <input name="n" ng-model="formData.n" type="text"/>
                    </div>
                </form>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-default" data-dismiss="modal">Cancel</button>
                <button type="button" class="btn btn-primary" ng-click="processForm()">Submit</button>
            </div>
        </div>
    </div>
</div>

<!-- 添加分组窗口-->
<div id="freeStoreDialog" class="modal fade" tabindex="-1" role="dialog" aria-labelledby="myLargeModalLabel"
     ng-controller="freeStore">
    <div class="modal-dialog modal-lg">

        <div class="modal-content">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span
                        aria-hidden="true">&times;</span></button>
                <h4 class="modal-title">Add Group</h4>
            </div>
            <div class="modal-body">
                <span ng-if="items.length == 0">no free store...</span>
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
                <button type="button" class="btn btn-default" data-dismiss="modal">Cancel</button>
                <button type="button" class="btn btn-primary" ng-click="addGroup()">Submit</button>
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
                <h4 class="modal-title">Add Volume</h4>
            </div>
            <div class="modal-body">
                <form id="addVolumeForm">
                    <input type="text" ng-model="formData.groupId" type="text" class="hide"/>
                    <fieldset>
                        <label>Number&nbsp;&nbsp;&nbsp;&nbsp;:</label>
                        <input name="n" ng-model="formData.n" type="text"/>
                    </fieldset>
                </form>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-default" data-dismiss="modal">Cancel</button>
                <button type="button" class="btn btn-primary" ng-click="processForm()">Submit</button>
            </div>
        </div>
    </div>
</div>

<div class=container>
    <div class="row">
        <div class="col-md-10">
            <h4>Racks View</h4>
        </div>
    </div>
    <div class="row" ng-controller="rack">
        <div class="col-md-11" ng-repeat="rack in items|orderBy:'name'">
            <div class="panel panel-default">
                <div class=panel-heading><b>{{rack.name}}</b>
                </div>
                <div class=panel-body>
                    <table class="table table-bordered">
                        <tr>
                            <th class="col-md-3">Store</th>
                            <th class="col-md-3">Ip</th>
                            <th class="col-md-3">Volumes</th>
                            <th class="col-md-2">Op</th>
                        </tr>
                        <tr ng-repeat="store in rack.stores|orderBy:'id'">
                            <td>{{store.id}}</td>
                            <td>{{store.ip}}</td>
                            <td><span ng-repeat="volumeId in store.volumes|orderBy">{{volumeId}},</span></td>
                            <td>
                                <button class="btn btn-xs btn-primary" ng-click="initFreeVolume(store.admin,store.id)">
                                    Init Volume
                                </button>
                                <button class="btn btn-xs btn-primary" ng-click="storeInfo(store.stat)">Stat</button>
                            </td>
                        </tr>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <div class=row>
        <div class=col-md-10>
            <h4>Groups View
                <button type="button" class="btn btn-sm btn-default" data-toggle="modal" data-target="#freeStoreDialog">
                    <span class="glyphicon glyphicon-plus"></span>Add Group
                </button>
            </h4>
        </div>
        <p>

        </p>
    </div>


    <div class="row" ng-controller="group">
        <div class="col-md-11">
            <div class="panel panel-default" ng-repeat="group in items|orderBy:'id'">
                <div class=panel-heading><b>group_{{ group.id }}</b>

                    <div class="btn-group btn-group-sm pull-right">
                        <button class="btn btn-sm btn-default" ng-click="openDailog(group.id)"
                                title="Add Redis Server"><span class="glyphicon glyphicon-plus"></span>
                            Add Volume
                        </button>
                    </div>
                </div>
                <div class=panel-body>
                    <table class="table table-bordered">
                        <tr>
                            <th class="col-md-3">Store</th>
                            <th class="col-md-3">Ip</th>
                            <th class="col-md-3">Volumes</th>
                            <th class="col-md-2">Op</th>
                        </tr>
                        <tr ng-repeat="store in group.stores|orderBy:'id'">
                            <td>{{store.id}}</td>
                            <td>{{store.ip}}</td>
                            <td><span ng-repeat="volumeId in store.volumes|orderBy">{{volumeId}},</span></td>
                            <td>
                                <button class="btn btn-xs btn-primary" ng-click="initFreeVolume(store.admin,store.id)">
                                    Init Volume
                                </button>
                                <button class="btn btn-xs btn-primary" ng-click="storeInfo(store.stat)">Stat</button>
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
