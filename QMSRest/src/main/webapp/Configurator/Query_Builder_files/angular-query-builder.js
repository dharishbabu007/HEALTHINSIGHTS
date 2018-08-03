var app = angular.module('app', ['ngSanitize', 'queryBuilder']);
app.controller('QueryBuilderCtrl', ['$scope', '$rootScope', function ($scope, $rootScope) {
	//var data = '{"group": {"operator": "AND","rules": []}}';
    var data = '{"group": {"rules": []}}';
	
    function htmlEntities(str) {
        return String(str).replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function computed(group) {			
        if (!group) return "";
        for (var str = "(", i = 0; i < group.rules.length; i++) {
            //i > 0 && (str += " <strong>" + group.operator + "</strong> ");			
			i > 0 && (str += " ");			
            str += group.rules[i].group ?
                computed(group.rules[i].group) :
                //group.rules[i].field + " " + htmlEntities(group.rules[i].condition) + " " +group.rules[i].data;				
				group.rules[i].operator + " " + htmlEntities(group.rules[i].businessExpression);
        }
		
        return str + ")";
    }
	
	
	//for save and submit begin
	var selectedMeasureId = $rootScope.measureId;
	$scope.submit = function (action) {
		if(!angular.isDefined($scope.configType)) {
			alert(" Please select the type..");
			return;
		}
		for (i = 0; i < $scope.group.rules.length; i++) {
			alert(i+ " data " + $scope.group.rules[i].businessExpression);
		} 		
		
		$scope.restResult = {status : "", meaasge : ""};
		$http.post('http://localhost:8081/curis/measure_configurator/'+selectedMeasureId+'/'+$scope.configType, 
		$scope.group.rules)		
		.then(function successCallback(response) {
			$scope.restResult = response.data;				
			console.log(response.data);			
			alert($scope.restResult.status + " : " + $scope.restResult.message);
		}, function errorCallback(response) {
			console.log(response.statusText);
		});	 	
	}
	//for save and submit end
	
	
    $scope.json = null;

    $scope.filter = JSON.parse(data);
	alert(" data " + data);
    $scope.$watch('filter', function (newValue) {
        $scope.json = JSON.stringify(newValue, null, 2);
        $scope.output = computed(newValue.group);
    }, true);
}]);

var queryBuilder = angular.module('queryBuilder', []);
queryBuilder.directive('queryBuilder', ['$compile', function ($compile) {
    return {
        restrict: 'E',
        scope: {
            group: '='			
        },
		controller: function ($scope) {  			
			$scope.$parent.group = $scope.group;
        },		
		templateUrl: '/queryBuilderDirective.html',		
        compile: function (element, attrs) {
            var content, directive;
            content = element.contents().remove();
            return function (scope, element, attrs) {
                scope.operators = [
                    { name: 'AND' },
                    { name: 'OR' },
					{ name: 'UNION' }					
                ];

				scope.buttonTab = true;
                scope.addCondition = function () {					
                    scope.group.rules.push({
                        //condition: '=',
                        operator: 'AND',
						businessExpression: 'Technical Expression',
						technicalExpression: 'Business Expression'
                        //data: ''
                    });										
                }; 

                scope.removeCondition = function (index) {
                    scope.group.rules.splice(index, 1);					
                }; 
				
                directive || (directive = $compile(content));

                element.append(directive(scope, function ($compile) {
                    return $compile;
                }));
            }
        }
    }
}]);
