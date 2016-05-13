Array.prototype.remove = function (b) {
    var a = this.indexOf(b);
    if (a >= 0) {
        this.splice(a, 1);
        return true;
    }
    return false;
};

var ng = angular
var myapp = ng.module('myapp', []);

var storeInfo = function (host) {
    window.open("http://" + host + "/info")
}
var initFreeVolume = function(host,storeId){
    var dailog = $('#initFreeVolumeDialog');
    var scope = dailog.scope();
    var formData = {}

    scope.formData = formData;
    scope.storeId = storeId
    formData.host=host;


    dailog.modal('show');
}

myapp.controller('rack', function ($scope, $http) {
    $http.get("/rack").success(function (data) {
        var items = eval(data)
        $scope.items = items;
    })

    $scope.storeInfo = storeInfo
    $scope.initFreeVolume = initFreeVolume
})

myapp.controller("initFreeVolume",function($scope,$http){
    $scope.processForm = function() {
        $http({
            method: "POST",
            url: "/addFreeVolume",
            params:$scope.formData
        }).success(function(data){
            var json = eval(data)
            if (json.success){
                alert("操作成功");
                var dailog = $('#initFreeVolumeDialog');
                dailog.modal('hide');
            } else {
                alert(json.msg);
            }
        })
    }
})

myapp.controller('freeStore', function ($scope, $http) {

    $http.get("/freeStore").success(function (data) {
        var items = eval(data)
        $scope.items = items;
        var stores = [];
        $scope.toggleChecked = function (flag, storeId) {
            if (flag) {
                stores.push(storeId)
            } else {
                stores.remove(storeId)
            }
        }

        $scope.addGroup = function () {
            $http({
                method: "POST",
                url: "/group",
                params: {"stores": stores.join(","), "racks": 2, "copys": 2}
            }).success(function (data) {
                if (data.success){
                    alert("操作成功")
                    var dailog = $('#freeStoreDialog');
                    dailog.modal('hide');
                }
            })
        }
    })

})

myapp.controller('group', function ($scope, $http) {
    $http.get("/group").success(function (data) {
        var items = eval(data)
        $scope.items = items;
    })

    $scope.storeInfo = storeInfo
    $scope.initFreeVolume = initFreeVolume

    $scope.openDailog= function(groupId){

        var dailog = $('#addVolumeDialog');
        var scope = dailog.scope();
        var formData = {}
        scope.formData = formData;
        formData.groupId=groupId;


        dailog.modal('show');
    }
})


myapp.controller("addVolume",function($scope,$http){
    $scope.processForm = function() {
        $http({
            method: "POST",
            url: "/addVolume",
            params:$scope.formData
        }).success(function(data){
            var json = eval(data)
            if (json.success){
                alert("操作成功");
                var dailog = $('#addVolumeDialog');
                dailog.modal('hide');
            } else {
                alert(json.msg);
            }
        })
    }
})
