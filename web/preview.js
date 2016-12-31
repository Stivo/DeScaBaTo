var app = angular.module('previewer', ['ui.bootstrap']);

var isEditableFilePattern = /\.(csv|txt|diff?|patch|svg|asc|cnf|cfg|conf|html?|.html|cfm|cgi|aspx?|ini|pl|py|md|css|cs|js|jsp|log|htaccess|htpasswd|gitignore|gitattributes|env|json|atom|eml|rss|markdown|sql|xml|xslt?|sh|rb|as|bat|cmd|cob|for|ftn|frm|frx|inc|lisp|scm|coffee|php[3-6]?|java|c|cbl|go|h|scala|vb|tmpl|lock|go|yml|yaml|tsv|lst)$/i;
var isImageFilePattern = /\.(jpe?g|gif|bmp|png|svg|tiff?)$/i;
var isExtractableFilePattern = /\.(gz|tar|rar|g?zip)$/i;

app.controller('PreviewCtrl', function ($scope, $window, $location, $http) {
    $scope.url = URI.parseQuery(location.search).path;

    $scope.active = -1;

    if (isEditableFilePattern.test($scope.url)) {
        $scope.type = "text";
    } else if (isImageFilePattern.test($scope.url)) {
        $scope.type = "image";
    } else {
        $scope.type = undefined;
    }

    if ($scope.type === "text" || $scope.type == undefined) {
        $http.get('/api/preview/'+$scope.url).then(function(response) {
            $scope.text = response.data;
        }, function(response) {
            $scope.text = response.statusText + "\n" + response.data;
        });
        if ($scope.type === "text") {
            $scope.hideTabs = true;
        } else {
            $scope.active = 0;
        }
    } else if ($scope.type === "image") {
        $scope.hideTabs = true;
    }

    $scope.tabs = [
      { title:'Preview as Text', id: "text" },
      { title:'Preview as Image', id: "image" }
    ];

    $scope.model = {
      name: 'Tabs'
    };
});
