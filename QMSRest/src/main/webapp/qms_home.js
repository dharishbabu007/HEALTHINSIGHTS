            var app = angular.module("QMSHomeManagement", ["ngRoute","ngSanitize","queryBuilder"]);
			var baseURL = 'http://localhost:8082/curis'; //local server
			//var baseURL = 'http://192.168.184.70:8082/curis'; //DC server
			//var baseURL = 'http://healthinsight:8082/curis';
			//var baseURL = 'http://104.211.216.183:8082/curis'; //Azure server
			app.config(function($routeProvider,$locationProvider) {
				$routeProvider
				.when("/", {
					templateUrl : "qms_landing.jsp",
					controller : "QMSHomeController"
					
				})
				.when("/Final_Creator", {
					templateUrl : "QMS_Create_Creator.jsp",
					controller : "MeasureCreateController"
				})
				.when("/Measure_Library", {
					templateUrl : "QMS_measure_list.jsp",
					controller : "MeasureListController"
					
				})
				.when("/Measure_Worklist", {
					templateUrl : "QMS_Measure_Worklist.jsp",
					controller : "WorklistController"
				})
				.when("/Measure_Editor", {
					templateUrl : "QMS_Edit_Creator.jsp",
					controller : "MeasureEditController"
				})
				.when("/Measure_Configurator", {
					templateUrl : "QMS_Measure_Configurator.jsp",
					controller : "QueryBuilderCtrl"
				})
				.when("/Program_Creator", {
					templateUrl : "Program_Creator.jsp",
					controller : "ProgramCreatorCtrl"
				})
				.when("/Patient_Profile", {
					templateUrl : "QMS_Patient_Profile.jsp",
					controller : "PatientProfileController"
				})
				.when("/MemberDetails_List", {
					templateUrl : "QMS_Member_Details.jsp",
					controller : "MemberDetailsController"
				});				

			});
			
            //Controller Part
            app.controller("QMSHomeController", function($scope, $rootScope, $http) {
			
                $scope.reimbursementPrograms = [];
				$scope.clinicalConditions = [];
				$scope.nqfDomains = [];
				
                //Now load the data from server
				_listReimbursementPrograms();
				_listClinicalConditions();
				_listNQFDomains();
				
                function _listReimbursementPrograms() {
                    $http({
                        method : 'GET',
                        //url : baseURL+'/qms/dropdown_list/dim_quality_measure_main/program_name'
						url : baseURL+'/qms/qmshome_dropdown_list/QMS_MEASURE/PROGRAM_NAME'
                    }).then(function successCallback(response) {
                        $scope.reimbursementPrograms = response.data;
                    }, function errorCallback(response) {
                        console.log(response.statusText);
                    });
                }         

                function _listClinicalConditions() {
                    $http({
                        method : 'GET',
                        //url : baseURL+'/qms/dropdown_list/dim_quality_measure_main/clinical_conditions'
						url : baseURL+'/qms/qmshome_dropdown_list/QMS_MEASURE/CLINICAL_CONDITIONS'
                    }).then(function successCallback(response) {
                        $scope.clinicalConditions = response.data;
                    }, function errorCallback(response) {
                        console.log(response.statusText);
                    });
                } 



				$scope.nqfDomains = {name : "", value : ""};
				function _listNQFDomains() {
					$http({
						method : 'GET',
						url : baseURL+'/qms/dropdown_namevalue_list/QMS_MEASURE_DOMAIN/MEASURE_DOMAIN_ID/MEASURE_DOMAIN_NAME'
					}).then(function successCallback(response) {
						$scope.nqfDomains = response.data;
					}, function errorCallback(response) {
						console.log(response.statusText);
					});
				}				
                function _listNQFDomains_notUsed() {
                    $http({
                        method : 'GET',
                        //url : baseURL+'/qms/dropdown_list/dim_quality_measure_main/nqs_domain'
						url : baseURL+'/qms/qmshome_dropdown_list/QMS_MEASURE/DOMAIN'
                    }).then(function successCallback(response) {
                        $scope.nqfDomains = response.data;						
                    }, function errorCallback(response) {
                        console.log(response.statusText);
                    });
                }  
				
				$scope.qmsHomeIconClick = function(type, value){
					$rootScope.programType = type;
					$rootScope.programValue = value;
					window.location.href="hleft.jsp#!/Measure_Library";
				};				
				
            });
			
			//Measure List
            //Controller Part
            app.controller("MeasureListController", function($scope, $rootScope, $http, $location, $window) {
      
				var filterType = $rootScope.programType; //$location.search().type;
				var filterValue = $rootScope.programValue; //$location.search().value;
				$rootScope.programType = "all";
				$rootScope.programValue = "all";
				
                $scope.measureList = [];
				$scope.selectedId = -1;
				$scope.selectedProgramName = "";
				$scope.selectedMeasureName = "";
				$scope.measureForm = {
                    id : -1,
                    name : "",
                    programName : "",
					type : "",
					steward : "",
					clinocalCondition : "",
					status : "",
					reviewComments : "",
					reviewedBy : ""
                };	

				_listMeasures();	
				
				//_listMeasures();				
                function _listMeasures() {
                    $http({
                        method : 'GET',
                        url : baseURL+'/qms/measure_list/'+filterType+'/'+filterValue
                    }).then(function successCallback(response) {
                        $scope.measureList = response.data;						
                    }, function errorCallback(response) {
                        console.log(response.statusText);
                    });
                }  

				$scope.viewMeasureLib = function (measure) {
					$scope.selectedId = measure.id;	
					$scope.selectedProgramName = measure.programName;	
					$scope.selectedMeasureName = measure.name;	
				}
				
				$scope.dashboardClick = function (measure) {
					console.log("came here")
					$scope.selectedProgramName = measure.programName;
					$scope.selectedMeasureName = measure.name;
					
					if($scope.selectedProgramName == "Merit-Based Incentive Payment System (MIPS) Program" && $scope.selectedMeasureName == "Comprehensive Diabetes Care: Eye Exam") 
						window.location.href = "MIPS_Dia.html";
					if($scope.selectedProgramName == "Merit-Based Incentive Payment System (MIPS) Program" && $scope.selectedMeasureName == "Breast Cancer Screening") 
						window.location.href = "MIPS_BCS.html";
					if($scope.selectedProgramName == "The Healthcare Effectiveness Data and Information Set (HEDIS)" && $scope.selectedMeasureName == "Comprehensive Diabetes: Eye Exam") 
						window.location.href = "HEDIS_Dia.html";
					if($scope.selectedProgramName == "The Healthcare Effectiveness Data and Information Set (HEDIS)" && $scope.selectedMeasureName == "Well-care Child Visit: 3-6 years") 
						window.location.href = "HEDIS_Well.html";
				}
				
				$scope.viewEditClick = function (action) {	
					$rootScope.measureId = $scope.selectedId;
					$rootScope.action = action;
					//$rootScope.source = "measureWorkList";
					$rootScope.source = "measureLibrary";
					
					//window.location.href="QMS_Edit_Creator.jsp#?measureId="+$scope.selectedId+"&action="+action+"&source=measureLibrary";
					//window.location.href="#!Measure_Editor#?measureId="+$scope.selectedId+"&action="+action+"&source=measureLibrary";
					window.location.href="hleft.jsp#!/Measure_Editor";
					//$location.path('/Measure_Editor');
				}
				
				$scope.copyClick = function (action) {
					
					$rootScope.measureId = $scope.selectedId;
					$rootScope.action = action;
					//$rootScope.source = "measureWorkList";
					$rootScope.source = "measureLibrary";
					
					//window.location.href="QMS_Create_Creator.jsp#?measureId="+$scope.selectedId+"&action="+action+"&source=measureLibrary";	
					window.location.href="hleft.jsp#!/Final_Creator";
				}
                   


                $scope.setSelected = function(measure) {
				   console.log("show", arguments, this);
				   if ($scope.lastSelected) {
					 $scope.lastSelected.selected = '';					 
				   }
				   this.selected = 'highlight';
				   $scope.lastSelected = this;
				   $scope.selectedId=measure.id;
				
				}
				//row select
/*
				$scope.setSelected = function() {
				   console.log("show", arguments, this);
				   if ($scope.lastSelected) {
					 $scope.lastSelected.selected = '';
				   }
				   this.selected = 'highlight';
				   $scope.lastSelected = this;
				}*/
				$scope.dblClickLibrary = function (action) {
                    $rootScope.measureId = $scope.selectedId;
                    $rootScope.action = action;
                    $rootScope.source = "measureLibrary";                   
                    //window.location.href="QMS_Edit_Creator.jsp#?measureId="+$scope.selectedId+"&action="+action+"&source=measureWorkList";    
                    window.location.href="hleft.jsp#!/Measure_Editor";
                }
					
            });	
			
			
			
			
			//var app = angular.module("QMSHomeManagement", []);
			//Worklist
            app.controller("WorklistController", function($scope, $rootScope, $http, $location, $window) {					
				
                $scope.workList = [];
				$scope.workForm = {
					 id : "",
					 programName : "",
					 name : "",
					 description : "",
					 measureDomain : "",
					 measureCategory : "",
					 type : "",
					 clinocalCondition : "",
					 targetAge : "",
					 numerator : "",
					 denominator : "",
					 numeratorExclusions : "",
					 denomExclusions : "",
					 steward : "",
					 dataSource : "",
					 target : "",
					 status : "",
					 reviewComments : "",
					 reviewedBy : ""
                };	

				_listWorklist();	
				
				//_listWorkflows();				
                function _listWorklist() {
                    $http({
                        method : 'GET',
                        url : baseURL+'/qms/work_list/'
                    }).then(function successCallback(response) {
                        $scope.workList = response.data;
                    }, function errorCallback(response) {
                        console.log(response.statusText);
                    });
                }   

				//row select
				$scope.selectedId = -1;
				$scope.selectedMeasureTitle = "";
				$scope.setSelected = function(workList) {
				   console.log("show", arguments, this);
				   if ($scope.lastSelected) {
					 $scope.lastSelected.selected = '';					 
				   }
				   this.selected = 'highlight';
				   $scope.lastSelected = this;
				   $scope.selectedId=workList.id;
				   $scope.selectedMeasureTitle = workList.name;
				   
				   if(workList.status == "Approved")  {
					   $scope.isDisabled = false;
				   } else {
					   $scope.isDisabled = true;
				   }
				}
				
				$scope.restResult = {status : "", meaasge : ""};
				//reject, approve Click
				$scope.statusClick = function (status) {					
					$http.put(baseURL+'/qms/work_list/status/'+$scope.selectedId+'/'+status)		
					.then(function successCallback(response) {
						$scope.restResult = response.data;
						console.log(response.statusText);
						_listWorklist();
						//alert("Status update success. ");
						swal($scope.restResult.status + " : " + $scope.restResult.message);
					}, function errorCallback(response) {
						console.log(response.statusText);
					});
				}							
				
				$scope.statusClickImg = function (status, workFlowId) {					
					$http.put(baseURL+'/qms/work_list/status/'+workFlowId+'/'+status)		
					.then(function successCallback(response) {
						$scope.restResult = response.data;
						console.log(response.statusText);
						_listWorklist();
						//alert("Status update success. ");
						swal($scope.restResult.status + " : " + $scope.restResult.message);
					}, function errorCallback(response) {
						console.log(response.statusText);
					});
				}											

				//view button
				$scope.viewClick = function (action) {
					$rootScope.measureId = $scope.selectedId;
					$rootScope.action = action;
					$rootScope.source = "measureWorkList";					
					//window.location.href="QMS_Edit_Creator.jsp#?measureId="+$scope.selectedId+"&action="+action+"&source=measureWorkList";	
					window.location.href="hleft.jsp#!/Measure_Editor";
				}
				
				$scope.dblClickWorkList = function (action) {
					$rootScope.measureId = $scope.selectedId;
					$rootScope.action = action;
					$rootScope.source = "measureWorkList";					
					//window.location.href="QMS_Edit_Creator.jsp#?measureId="+$scope.selectedId+"&action="+action+"&source=measureWorkList";	
					window.location.href="hleft.jsp#!/Measure_Editor";					
				}

				
				
				$scope.isDisabled = true;
				$scope.measureConfigClick = function () {
					//$rootScope.measureId = $scope.selectedId;
					//window.location.href="hleft.jsp#!/Measure_Configurator";
					
					$http({
						method : 'GET',
						url : baseURL+'/measure_configurator/config_data/qms_input/qms_input123'
					}).then(function successCallback(response) {
						$scope.tableData = response.data;
						$window.sessionStorage.setItem("tableData", JSON.stringify($scope.tableData));
						
						$rootScope.measureId = $scope.selectedId;
						$rootScope.selectedMeasureTitle = $scope.selectedMeasureTitle;
						window.location.href="hleft.jsp#!/Measure_Configurator";						
					}, function errorCallback(response) {
						console.log(response.statusText);
					});
				}
				$scope.measureConfigImgClick = function (selectedId, selectedMeasureTitle,selectedMeasureStatus) {
					//$rootScope.measureId = $scope.selectedId;
					//window.location.href="hleft.jsp#!/Measure_Configurator";
					$scope.MeasureStatus = selectedMeasureStatus;
					if($scope.MeasureStatus == "Review"){
						$scope.enable = true;
					}
					$http({
						method : 'GET',
						url : baseURL+'/measure_configurator/config_data/qms_input/qms_input123'
					}).then(function successCallback(response) {
						$scope.tableData = response.data;
						$window.sessionStorage.setItem("tableData", JSON.stringify($scope.tableData));
						
						$rootScope.measureId = selectedId;
						$rootScope.selectedMeasureTitle = selectedMeasureTitle;
						window.location.href="hleft.jsp#!/Measure_Configurator";						
					}, function errorCallback(response) {
						console.log(response.statusText);
					});
				}
            });	
			
			
			
			//MemberDetails
            app.controller("MemberDetailsController", function($scope, $rootScope, $http, $location, $window) {	
			
				//member list
				$scope.memberDetailsList = [];
				_listMemberDetailslist();	
                function _listMemberDetailslist() {
                    $http({
                        method : 'GET',
                        url : baseURL+'/qms/spv/hedis_member_list/'
                    }).then(function successCallback(response) {
                        $scope.memberDetailsList = response.data;
                    }, function errorCallback(response) {
                        console.log(response.statusText);
                    });
                }	
				
				
				//Get spv details
                $scope.viewMemberDetail = function (member) {
                    $http({
                        method : 'GET',
						url : baseURL+'/qms/spv/hedis/'+member.id
                    }).then(function successCallback(response) {
						$window.sessionStorage.setItem("patientProfileData",JSON.stringify(response.data));
						window.location.href="QMS_Patient_Profile.jsp";
                    }, function errorCallback(response) {
						console.log(response.statusText);
                    });
                }					
				
            });	
			
			
			
			
	//var app = angular.module("QMSHomeManagement", []);
	//Worklist   
	app.controller("MeasureCreateController", function($scope, $rootScope, $http, $location) {
				
		//var selectedMeasureId = $location.search().measureId;
		//var action = $location.search().action;
		var selectedMeasureId = $rootScope.measureId;  //$location.search().measureId;
		var action = $rootScope.action;  //$location.search().action;		
		
		$scope.measureProgramNames = {name : "", value : ""};
		$scope.programNameCategory = {name : "", value : ""};
		_listMeasureProgramNames();		
		function _listMeasureProgramNames() {
			$http({
				method : 'GET',				
				url : baseURL+'/qms/dropdown_namevalue_list/qms_quality_program/PROGRAM_ID/PROGRAM_NAME'
			}).then(function successCallback(response) {
				$scope.measureProgramNames = response.data;
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
			
			$http({
				method : 'GET',				
				url : baseURL+'/qms/dropdown_namevalue_list/qms_quality_program/PROGRAM_NAME/CATEGORY_NAME'
			}).then(function successCallback(response) {
				$scope.programNameCategory = response.data;
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
			
		}		
		
		
		$scope.measureCategories = [];
		$scope.onChangeProgramName = function () {
			$scope.measureCategories = [];			
			for (var i in $scope.programNameCategory) {				
				if($scope.programNameCategory[i].value == $scope.measureForm.programName)
					$scope.measureCategories.push($scope.programNameCategory[i].name);
			}
		}
	
		
		$scope.measureTypes = {name : "", value : ""};
		_listMeasureTypes();
		function _listMeasureTypes() {
			$http({
				method : 'GET',
				url : baseURL+'/qms/dropdown_namevalue_list/QMS_MEASURE_TYPE/MEASURE_TYPE_ID/MEASURE_TYPE_NAME'
			}).then(function successCallback(response) {
				$scope.measureTypes = response.data;
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}		
		
		$scope.measureDomains = {name : "", value : ""};
		_listMeasureDomains();
		function _listMeasureDomains() {
			$http({
				method : 'GET',
				url : baseURL+'/qms/dropdown_namevalue_list/QMS_MEASURE_DOMAIN/MEASURE_DOMAIN_ID/MEASURE_DOMAIN_NAME'
			}).then(function successCallback(response) {
				$scope.measureDomains = response.data;
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}				
		
		$scope.measureForm = {					 
			id : -1,
			name : "",
			programName : "",
			type : "",
			steward : "",
			clinocalCondition : "",
			status : "",
			reviewComments : "",
			reviewedBy : "",
			targetAge : "",
			measureDomain : "",
			measureCategory : "",
			target : "",
			description : "",
			denominator : "",
			denomExclusions : "",
			numerator : "",
			numeratorExclusions : "",
			measureEditId : ""
		};	
		
		$scope.$on('emptyMeasureCreatorObj', function (event, args) {
			$scope.measureForm={};
        })		
				
		if(action == "copy") {
			_getMeasureLib();
			$rootScope.measureId = "";
			$rootScope.action = "";
		}
		
		function _getMeasureLib() {
			$http({
				method : 'GET',
				url : baseURL+'/qms/measure_list/'+selectedMeasureId
			}).then(function successCallback(response) {
				$scope.measureForm = response.data;
				console.log(" measure --> " + $scope.measureForm);
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}
		
		$scope.restResult = {status : "", meaasge : ""};
		$scope.submitAdd = function (action) {	
			if($scope.measureForm.programName == "" || !angular.isDefined($scope.measureForm.programName))  {

			swal(" Please enter the value for Measure Program.");
				return;
			}
			if($scope.measureForm.name == "" || !angular.isDefined($scope.measureForm.name))  {
				swal(" Please enter the value for Measure Title.");
				return;
			}
			if($scope.measureForm.measureCategory == "" || !angular.isDefined($scope.measureForm.measureCategory))  {
				swal(" Please select the value for Measure Category.");
				return;
			}
			$scope.measureForm.measureEditId = "";
			if(action=="save")
				$scope.measureForm.status = "In-Progress";
			if(action=="submit")
				$scope.measureForm.status = "Review";
			
			$http.post(baseURL+'/qms/work_list/', $scope.measureForm)		
			.then(function successCallback(response) {
				$scope.restResult = response.data;
				console.log(response.statusText);
				if(action=="submit" && $scope.restResult.status=='SUCCESS')
					_clearFormData();
				swal($scope.restResult.status + " : " + $scope.restResult.message);
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}

		function _clearFormData() {
			$scope.measureForm.id = -1;
			$scope.measureForm.name = "";
			$scope.measureForm.programName = "";
			$scope.measureForm.type = "";
			$scope.measureForm.steward = "";
			$scope.measureForm.clinocalCondition = "";
			$scope.measureForm.status = "";
			$scope.measureForm.reviewComments = "";
			$scope.measureForm.reviewedBy = "";
			$scope.measureForm.targetAge = "";
			$scope.measureForm.measureDomain = "";
			$scope.measureForm.measureCategory = "";
			$scope.measureForm.target = "";
			$scope.measureForm.description = "";
			$scope.measureForm.denominator = "";
			$scope.measureForm.denomExclusions = "";
			$scope.measureForm.numerator = "";
			$scope.measureForm.numeratorExclusions = "";				 			
			$scope.measureForm.measureEditId = "";				 			
		}
	});	
	
	
	
	
	
	
	//var app = angular.module("QMSHomeManagement", []);
	//Editor Controller
	app.controller("MeasureEditController", function($scope, $rootScope, $http, $location) {					
		
		var selectedMeasureId = $rootScope.measureId;  //$location.search().measureId;
		var action = $rootScope.action;  //$location.search().action;
		var source = $rootScope.source;  //$location.search().source;
		
		$scope.measureForm = {					 
			id : -1,
			name : "",
			programName : "",
			type : "",
			steward : "",
			clinocalCondition : "",
			status : "",
			reviewComments : "",
			reviewedBy : "",
			targetAge : "",
			measureDomain : "",
			measureCategory : "",
			target : "",
			description : "",
			denominator : "",
			denomExclusions : "",
			numerator : "",
			numeratorExclusions : "",
			sourceType : ""		
		};	

		_getMeasureLib();	
		
		function _getMeasureLib() {
			if(source == "measureLibrary") {
				$http({
					method : 'GET',
					url : baseURL+'/qms/measure_list/'+selectedMeasureId
				}).then(function successCallback(response) {
					$scope.measureForm = response.data;
					console.log(" measure --> " + $scope.measureForm);
				}, function errorCallback(response) {
					console.log(response.statusText);
				});
			}
			else if(source == "measureWorkList") {
				$http({
					method : 'GET',
					url : baseURL+'/qms/work_list/'+selectedMeasureId
				}).then(function successCallback(response) {
					$scope.measureForm = response.data;
					console.log(" measure --> " + $scope.measureForm);
				}, function errorCallback(response) {
					console.log(response.statusText);
				});
			}			
		}   
				
		//enable the input fields
		$scope.isDisabled = true;
		$scope.editClick = function() {
			$scope.isDisabled = false;
		}
		
		$scope.restResult = {status : "", meaasge : ""};
		$scope.submitEdit = function (action) {			
			if($scope.measureForm.programName == "" || !angular.isDefined($scope.measureForm.programName))  {
				swal(" Please select the value for Program Name.");
				return;
			}
			if($scope.measureForm.name == "" || !angular.isDefined($scope.measureForm.name))  {
				swal(" Please enter the value for Measure Title.");
				return;
			}
			if($scope.measureForm.measureCategory == "" || !angular.isDefined($scope.measureForm.measureCategory))  {
				swal(" Please select the value for Measure Category.");
				return;
			}			
			$scope.measureForm.sourceType = source;
			if(action=="save")
				$scope.measureForm.status = "In-Progress";
			if(action=="submit")
				$scope.measureForm.status = "Review";
		
			$http.put(baseURL+'/qms/work_list/'+selectedMeasureId, 
			$scope.measureForm)		
			.then(function successCallback(response) {
				$scope.restResult = response.data;				
				console.log(response.data);
				if(action=="submit" && $scope.restResult.status=='SUCCESS')
				_clearFormData();
				swal($scope.restResult.status + " : " + $scope.restResult.message);
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}	

		function _clearFormData() {
			$scope.measureForm.id = -1;
			$scope.measureForm.name = "";
			$scope.measureForm.programName = "";
			$scope.measureForm.type = "";
			$scope.measureForm.steward = "";
			$scope.measureForm.clinocalCondition = "";
			$scope.measureForm.status = "";
			$scope.measureForm.reviewComments = "";
			$scope.measureForm.reviewedBy = "";
			$scope.measureForm.targetAge = "";
			$scope.measureForm.measureDomain = "";
			$scope.measureForm.measureCategory = "";
			$scope.measureForm.target = "";
			$scope.measureForm.description = "";
			$scope.measureForm.denominator = "";
			$scope.measureForm.denomExclusions = "";
			$scope.measureForm.numerator = "";
			$scope.measureForm.numeratorExclusions = "";				 			
		}	

		$scope.measureProgramNames = {name : "", value : ""};
		$scope.programNameCategory = {name : "", value : ""};
		_listMeasureProgramNames();		
		function _listMeasureProgramNames() {
			$http({
				method : 'GET',				
				url : baseURL+'/qms/dropdown_namevalue_list/qms_quality_program/PROGRAM_ID/PROGRAM_NAME'
			}).then(function successCallback(response) {
				$scope.measureProgramNames = response.data;
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
			
			$http({
				method : 'GET',				
				url : baseURL+'/qms/dropdown_namevalue_list/qms_quality_program/PROGRAM_NAME/CATEGORY_NAME'
			}).then(function successCallback(response) {
				$scope.programNameCategory = response.data;
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
			
		}		
		
		
		$scope.measureCategories = [];
		$scope.onChangeProgramName = function () {
			$scope.measureForm.measureCategory = "";
			$scope.measureCategories = [];			
			for (var i in $scope.programNameCategory) {				
				if($scope.programNameCategory[i].value == $scope.measureForm.programName)
					$scope.measureCategories.push($scope.programNameCategory[i].name);
			}
		}
	
		
		$scope.measureTypes = {name : "", value : ""};
		_listMeasureTypes();
		function _listMeasureTypes() {
			$http({
				method : 'GET',
				url : baseURL+'/qms/dropdown_namevalue_list/QMS_MEASURE_TYPE/MEASURE_TYPE_ID/MEASURE_TYPE_NAME'
			}).then(function successCallback(response) {
				$scope.measureTypes = response.data;
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}

		$scope.measureDomains = {name : "", value : ""};
		_listMeasureDomains();
		function _listMeasureDomains() {
			$http({
				method : 'GET',
				url : baseURL+'/qms/dropdown_namevalue_list/QMS_MEASURE_DOMAIN/MEASURE_DOMAIN_ID/MEASURE_DOMAIN_NAME'
			}).then(function successCallback(response) {
				$scope.measureDomains = response.data;
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}
    });	
	
	
	//HLeft Controller
	app.controller("HLeftController", function($scope, $rootScope, $http, $location, $window) {		
		$scope.userName = $window.sessionStorage.getItem("loginName");

		$scope.logOut = function () {
			//$window.sessionStorage.setItem("loginName","");
			$window.sessionStorage.clear();
			window.location.href="QMS_login.jsp";
		}
		
		$scope.goToMeasureCreator = function () {
			$scope.$broadcast('emptyMeasureCreatorObj', { message: "Hello" });
			//window.location.href="hleft.jsp#!/Final_Creator";
		}		
		
		$scope.activeTab = '#!';		
		$scope.isActive = function (viewLocation) { 
			//return viewLocation === $location.path();
			$scope.activeTab = viewLocation;
			console.log($scope.activeTab)
		};
					
	});
	
	//IndexDotHTMLController Controller
	app.controller("IndexDotHTMLController", function($scope, $rootScope, $http, $location, $window) {		
		$scope.userName = $window.sessionStorage.getItem("loginName");

		$scope.logOut = function () {
			//$window.sessionStorage.setItem("loginName","");
			$window.sessionStorage.clear();
			window.location.href="QMS_login.jsp";
		}
		
		$scope.viewPatientProfile = function (patienceId) {
			
			$http({
				method : 'GET',
				//url : baseURL+'/qms/spv/'+patienceId
				url : baseURL+'/qms/spv/2014875'
			}).then(function successCallback(response) {
				$scope.patientProfileForm = response.data;
				console.log(" patient Profile Data --> " + $scope.patientProfileForm);
				$window.sessionStorage.setItem("patientProfileData",JSON.stringify($scope.patientProfileForm));
				window.location.href="QMS_Patient_Profile.jsp";
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}
		
	});	

	//Login Controller
	app.controller("LoginController", function($scope, $rootScope, $http, $location, $window) {

		$scope.userDetails = {
			id : "",
			name : "",
			loginId : "",
			email : "",
			roleId : ""
		}
		
		$scope.errorMessage = "";
			
		$scope.submit = function () {

			$http({
				method : 'GET',
				url : baseURL+'/qms/user/'+$scope.userName+'/'+$scope.password
			}).then(function successCallback(response) {
				$scope.userDetails = response.data;
				$window.sessionStorage.setItem("loginName",$scope.userName);
				window.location.href="index.html";
			}, function errorCallback(response) {
				console.log(response.statusText);
				$scope.errorMessage = "Invalid user name or password";
			});	 

			/**	
			alert($scope.userDetails.id + " 222 " + $scope.userDetails.loginId);
			if($scope.userDetails.id != "" && $scope.userDetails.loginId == $scope.userName) {
				$window.sessionStorage.setItem("loginName",$scope.userName);
				window.location.href="hleft.jsp";
			} else {
				$scope.errorMessage = "Invalid user name or password";
			} */
			
		}
		
	});	
	
	
app.controller("PatientProfileController", function($scope, $http, $location) {	
	swal("Test");
	var selectedMemberId = $location.search().mrnId;
	swal("selectedMemberId "+selectedMemberId);
	$scope.memberId = selectedMemberId;		
	$scope.patientProfileForm = { 
		 patId : "",
		 ptyId : "",
		 emrPatId : "",
		 ssn : "",
		 mrn : "",
		 firstName : "",
		 middleName : "",
		 lastName : "",
		 city : "",
		 addLine1 : "",
		 addLine2 : "",
		 state : "",
		 county : "",
		 country : "",
		 race : "",
		 zip : "",
		 deathDate : "",
		 birthDate : "",
		 emailAddress : "",
		 maritialStatus : "",
		 language : "",
		 gender : "",
		 lngtd : "",
		 lattd : "",	
		 ethniCity : "",
		 currFlag : "",
		 createDate : "",
		 updateDate : "",
		
		//for other fields - AggregateFactMember
		 comorbidity1 : "",
		 comorbidity2 : "",
		 comorbidity3 : "",
		 comorbidity4 : "",
		 comorbidity5 : "",
		 comorbidity6 : "",
		 comorbidity7 : "",
		 comorbidity8 : "",
		 comorbidity9 : "",
		 comorbidity10 : "",
		 careGaps1 : "",
		 careGaps2 : "",
		 careGaps3 : "",
		 careGaps4 : "",
		 ipVisitsCount : "",
		 opVisitsCount : "",
		 erVisitsCount : "",
		 prescription : "",
		 nextAppointmentDate : "",
		 physicianName : "",
		 department : "",
		 procedureName1 : "",
		 procedureDateTime1 : "",
		 procedureName2 : "",
		 procedureDateTime2 : "",			 
		 lastDateService : "",	
		 
		 //for provider details
		 providerFirstName : "",
		 providerLastName : "",
		 providerAddress1 : "",
		 providerAddress2 : "",
		 providerBillingTaxId : "",
		 providerSpeciality : "",

		 //new fileds added
		 address : "",
		 name : "",
		 phone : "",
		 age : "",
		 primaryPayer : "",
		 mraScore : "",
		 risk : ""
	};	

	_getPatientProfile();	
	
	function _getPatientProfile() {
		$http({
			method : 'GET',
			url : baseURL+'/qms/spv/'+memberId
		}).then(function successCallback(response) {
			$scope.patientProfileForm = response.data;
			console.log(" patient Profile Data --> " + $scope.patientProfileForm);
		}, function errorCallback(response) {
			console.log(response.statusText);
		});
	}   
});		
	
	
	
	
	//Measure Configuration controller
app.controller('QueryBuilderCtrl', ['$scope', '$rootScope', '$http', '$window', function ($scope, $rootScope, $http, $window) {
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
	
		//Getting table & column data to populate 
		//$scope.tableData = {};
		$scope.tableData = JSON.parse($window.sessionStorage.getItem("tableData"));
		//_getTechnicalExpressions();	
		function _getTechnicalExpressions() {
			$http({
				method : 'GET',
				url : baseURL+'/measure_configurator/config_data/qms_input/qms_input123'
			}).then(function successCallback(response) {
				$scope.tableData = response.data;				
			}, function errorCallback(response) {
				console.log(response.statusText);
			});	
		}
	
		//retrive and populate the measure configs
		var selectedMeasureId = $rootScope.measureId;			
		$scope.selectedMeasureTitle = $rootScope.selectedMeasureTitle;
		var measureConfigs = [];		
		$scope.categoryClick = function (configType) {
			$scope.group.rules = [];
			$scope.group.remarks = "";
			$http({
				method : 'GET',
				url : baseURL+'/measure_configurator/'+selectedMeasureId+'/'+configType
			}).then(function successCallback(response) {
				measureConfigs = response.data;			
				_getTechnicalExpressions();	
				var tableName;
				var colName;
				var	columnData;				
				for(var i=0; i < measureConfigs.length; i++) {					
					tableName = measureConfigs[i].technicalExpression.substring(0, measureConfigs[i].technicalExpression.indexOf('.'));
					colName = measureConfigs[i].technicalExpression.substring(measureConfigs[i].technicalExpression.indexOf('.')+1);
					columnData = [];					
					for (var t = 0; t < $scope.tableData.length; t++) {
						if($scope.tableData[t].name == tableName) {
							columnData = $scope.tableData[t].columnList;
							break;
						}
					}					
					
				    $scope.group.rules.push({
                        //condition: '=',
                        operator: measureConfigs[i].operator,
						businessExpression: measureConfigs[i].businessExpression,
						tableName: tableName,
						columnName: colName,
						columnData: columnData
						//technicalExpression: measureConfigs[i].technicalExpression
                    });
					$scope.group.remarks = measureConfigs[i].remarks;
				}
				
			}, function errorCallback(response) {
				console.log(response.statusText);
			});	 
		}
	
	
	/**
	$scope.technicalExpressions = [];
	$scope.tableData = {};
	_getTechnicalExpressions();	
	function _getTechnicalExpressions() {
			$http({
				method : 'GET',
				url : baseURL+'/measure_configurator/config_data/qms_input/qms_input123'
			}).then(function successCallback(response) {
				$scope.tableData = response.data;
				alert("$scope.tableData --> " + $scope.tableData.length);

				for (var i = 0; i < $scope.tableData.length; i++) {
					
					for (j = 0; j < $scope.tableData[i].columnList.length; j++) {
						$scope.technicalExpressions.push({columnName: $scope.tableData[i].name+'.'+$scope.tableData[i].columnList[j].name});
					} 
				}
				alert("$scope.technicalExpressions.length --> " + $scope.technicalExpressions.length);				
			}, function errorCallback(response) {
				console.log(response.statusText);
			});	
	} */
	
	
	
	//for save and submit begin
	$scope.submit = function (action) {
		if(!angular.isDefined($scope.configType)) {
			swal(" Please select the type..");
			return;
		}		
		if($scope.group.remarks == "" || $scope.group.remarks == null || $scope.group.remarks == '') {
			swal(" Please enter remarks ");
			return;
		}
		
		//adding remarks here
		for (i = 0; i < $scope.group.rules.length; i++) {
			$scope.group.rules[i].remarks = $scope.group.remarks;
			$scope.group.rules[i].status = action;			
			$scope.group.rules[i].technicalExpression = 
			$scope.group.rules[i].tableName + "." + $scope.group.rules[i].columnName;
		} 
		
		//sending data to server
		$scope.restResult = {status : "", meaasge : ""};
		$http.post(baseURL+'/measure_configurator/'+selectedMeasureId+'/'+$scope.configType, 
		$scope.group.rules)		
		.then(function successCallback(response) {
			$scope.restResult = response.data;				
			console.log(response.data);			
			swal($scope.restResult.status + " : " + $scope.restResult.message);
		}, function errorCallback(response) {
			console.log(response.statusText);
		});	 	
	}
	//for save and submit end
	
	
    $scope.json = null;

    $scope.filter = JSON.parse(data);
	
    $scope.$watch('filter', function (newValue) {
        $scope.json = JSON.stringify(newValue, null, 2);
        $scope.output = computed(newValue.group);
    }, true);
}]);

var queryBuilder = angular.module('queryBuilder', []);
app.directive('queryBuilder', ['$compile', '$http', function ($compile, $http) {
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
						businessExpression: 'Business Expression',
						//technicalExpression: 'Technical Expression',
						tableName: 'Table Name',
						columnName: 'Column Name',
						columnData: [],
						remarks: ''
                        //data: ''
                    });										
                }; 

                scope.removeCondition = function (index) {
                    scope.group.rules.splice(index, 1);					
                }; 
				
				scope.businessExpressionFocus = function ($event) {					
					$event.target.select();
				}
				
	
	//getting the technical expression data	
	//scope.technicalExpressions = [];
	scope.tableData = {};
	_getTechnicalExpressions();	
	function _getTechnicalExpressions() {
		$http({
			method : 'GET',
			url : baseURL+'/measure_configurator/config_data/qms_input/qms_input123'
		}).then(function successCallback(response) {
			scope.tableData = response.data;
			
			/**
			for (var i = 0; i < scope.tableData.length; i++) {
				
				for (j = 0; j < scope.tableData[i].columnList.length; j++) {
					scope.technicalExpressions.push({columnName: scope.tableData[i].name+'.'+scope.tableData[i].columnList[j].name});
				} 
			} */
		}, function errorCallback(response) {
			console.log(response.statusText);
		});	
	}

	scope.onChangeTableName = function (tableName, index) {
		for (var i = 0; i < scope.tableData.length; i++) {
			if(scope.tableData[i].name == tableName) {
				scope.group.rules[index].columnData = scope.tableData[i].columnList;
			}
		}		
	}
				
				
				
                directive || (directive = $compile(content));

                element.append(directive(scope, function ($compile) {
                    return $compile;
                }));
            }
        }
    }
}]);	
	
