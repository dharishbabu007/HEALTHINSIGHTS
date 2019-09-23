export const navItems = [
  {
    name: 'Applications',
    url: '#',
    icon: 'fa fa-line-chart',
    children: [
      {
        name: 'QualityManagement',
        url: '#',
        icon: 'fa fa-line-chart',
        children: [
          {
            name: 'Quality Central',
            url: '/Quality Central',
            icon: 'fa fa-line-chart'
          },
          { 
            name: 'Quality Program',
            url: 'http://healthinsight:8082/curis/hleft.jsp#!/',
            icon: 'fa fa-line-chart',
            children: [
              {
                name: 'MIPS',
                url: 'http://192.168.184.70/views/CurisDashboard_MIPS/MIPS_Control_Tower?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
                icon: 'fa fa-line-chart'
              },
              {
                name: 'HEDIS',
                url: 'http://192.168.184.70/views/HedisDashboard/Hedis?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
                icon: 'fa fa-line-chart'
              },
              // {
              //   name: 'OHA',
              //   url: '#',
              //   icon: 'fa fa-line-chart'
              // },
              // {
              //   name: 'CMS STAR',
              //   url: 'https://public.tableau.com/views/CMS_webscree_12dec/NEW_CMSSTAR?:embed=y&:showVizHome=n&:tabs=n&:toolbar=n&:apiID=host0#navType=0&navSrc=Parse',
              //   icon: 'fa fa-line-chart'
              // },
            ]
          },
          {
            name: 'Quality Measures',
            url: '/measurelibrary',
            icon: 'fa fa-line-chart'
          },
          {
            name: 'Member Compliance',
            url: 'http://192.168.184.70/views/Member_Measure_Compliance_Dashboard/MemberMeasure?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-line-chart'
          },
          {
            name: 'Configuration',
            url: '#',
            icon: 'fa fa-sliders',
            children: [
              {
                name: 'Program Creator',
                url: '/programcreator',
                icon: 'fa fa-sliders'
              },
             
              {
                name: 'Measure Creator',
                url: '/measurecreator',
                icon: 'fa fa-sliders'
              },
    
              {
                name: 'Program Editor',
                url: '/programeditor',
                icon: 'fa fa-sliders'
              },
              // {
              //   name: 'Configurator',
              //   url: '/configurator',
              //   icon: 'fa fa-sliders'
              // },
              {
                name: 'My Measures',
                url: '/measureworklist',
                icon: 'fa fa-sliders'
              },
            ]
          },
        ]
        // badge: {
        //   variant: 'info',
        //   text: 'NEW'
        // }
      },
      {
        name: 'Gaps in Care',
        url: '#',
        icon: 'fa fa-handshake-o',
        children: [
          {
            name: 'Care Summary',
            url: 'http://192.168.184.70/views/Gaps_in_Care_Dashboard1/CareGapSummaryDashboard?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-handshake-o'
          },
          {
            name: 'Gaps Registry',
            url: '/member-care-gap-list',
            icon: 'fa fa-handshake-o'
          },
          {
            name: 'Close Patient Gap',
            url: '/member-gap-list',
            icon: 'fa fa-handshake-o'
          },
          {
            name: 'MIT',
            url: '/pat-screen',
            icon: 'fa fa-handshake-o'
          }
        ]
      },
      {
        name: 'Chart Abstraction Tool',
        url: '/chart-abstract',
        icon: 'fa fa-handshake-o',
        },
        
        
      {
        name: 'PHM',
        url: '#',
        icon: 'fa fa-user-o',
        children: [
          {
            name: 'PHM SUMMARY',
            url: 'http://192.168.184.70/views/PHMsummary/SummaryDashboard?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-user-o'
          },
          {
            name: 'Risk Assessment',
            url: 'http://192.168.184.70/views/PHMRiskDashboard_0/PHMRisk_Dashboard?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-user-o'
          },
          {
            name: 'Measure Analysis',
            url: 'http://192.168.184.70/views/PHMMeasuressDashboard/Measures?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-user-o'
          },
          {
            name: 'Geo Analysis',
            url: 'http://192.168.184.70/views/Location_Dashboard/Template_2?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-user-o'
          },
          {
            name: 'Cohort',
            url: 'http://192.168.184.70/views/FinalDashboardPHM/RiskDashboard?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-user-o'
          },
          {
            name: 'Provider Analysis',
            url: 'http://192.168.184.70/views/Provider_Dashboard/ProviderDashboard?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-user-o'
          },
        ]
      },
      // {
      //   name: 'Health plan Employer portal',
      //   url: 'http://i3lbfsiserver:93/',
      //   icon: 'fa fa-line-chart',

      // },
      {
        name: 'CMS Reporting',
        url: '#',
        icon: 'fa fa-line-chart',
        children: 
        [
          {
            name: 'Part C',
            icon: 'fa fa-line-chart',
            children: [
              {
                name: 'Direct Member Reimbursement',
                url: 'http://192.168.184.70/views/DirectMemberReimbursementDMRRequestsPartCReportingDashboard/Dashboard1?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
                icon: 'fa fa-line-chart'
              },
              {
                name: 'Expedited Pre Service Organization Determinations',
                url: 'http://192.168.184.70/views/ExpeditedPre-serviceOrganizationDeterminationsEODPartCReportingDashboard/Dashboard1?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
                icon: 'fa fa-line-chart'
              }
            ]
          },
          {
            name: 'Part D',
            icon: 'fa fa-line-chart',
            children: [
              {
                name: 'Standard Coverage Determinations',
                url: 'http://192.168.184.70/views/StandardCoverageDeterminationsSCDPartDReportingDashboard/Dashboard1?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
                icon: 'fa fa-line-chart'
              },
              {
                name: 'Standard Grievances',
                url: 'http://192.168.184.70/views/StandardGrievancesPartDSGDPartDReportingDashboard/Dashboard1?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
                icon: 'fa fa-line-chart'
              }
            ]
          }

        ]
      },
      {
        name: 'Member Engagement',
        url: '#',
        icon: 'fa fa-line-chart',
        children: 
        [
          {
        name: 'Survey',
        url: 'https://s.surveyanyplace.com/s/ptheqvhs',
        icon: 'fa fa-line-chart',
          },
          {
            name: 'Healthy Me ',
            url: '/health/home',
            icon: 'fa fa-user-o',
          },

        ]
      }
      // {
      //   name: 'Provider',
      //   url: '#',
      //   icon: 'fa fa-line-chart',
      //   children: 
      //   [
      //     {
      //       name: 'Operations',
      //       url: '#',
      //       icon: 'fa fa-line-chart',
      //       children: 
      //   [
      //     {
      //       name: 'OR Utilisation',
      //       url: 'http://192.168.184.70/t/CurisSite/views/OR_Utilization_Template_webscreen/Summary?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
      //       icon: 'fa fa-line-chart',
      //     },
      //       {
      //       name: 'Stroke',
      //       url: 'http://192.168.184.70/t/CurisSite/views/Curis_Stroke/D2rTPAexamdash?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
      //       icon: 'fa fa-line-chart',
      //       },
      //       {
      //         name: 'OP Visits Summary',
      //         url: 'http://192.168.184.70/t/CurisSite/views/OutpatientAnalysis/OutpatientVisit?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
      //         icon: 'fa fa-line-chart',
      //       },
      //     ]
      //   },
      //   {
      //     name: 'Specialization',
      //     url: '#',
      //     icon: 'fa fa-line-chart',
      //     children: 
      //         [
      //       {
      //       name: 'Obstetrics',
      //       url: 'http://192.168.184.70/t/CurisSite/views/Curis_Obstetrics/Obstetrics_First?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
      //       icon: 'fa fa-line-chart',
      //       },
      //       {
      //         name: 'Mammography Quality',
      //         url: 'http://192.168.184.70/t/CurisSite/views/MammographyQuality/Mammo-Facility?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
      //         icon: 'fa fa-line-chart',
      //       }
      //   ]
      // }
      //   ]
      // },
    ]
  },
  {
    name: 'Analytical Utilities',
    url: '#',
    icon: 'fa fa-line-chart',
    children: 
    [ 
  

          {
            name: 'BI Workbench',
            url: 'http://192.168.184.70/views/SelfServiceBI/Dashboard1?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no',
            icon: 'fa fa-table',
          
          
          },
          
          {
            name: 'Development Studio',
            url: '#',
            icon: 'fa fa-line-chart',
            children: [
            {
            name: 'R Studio',
            url: 'http://192.168.184.71:8787/auth-sign-in',
            icon: 'fa fa-line-chart',
            },
            {
            name: 'Jupyter',
            url: 'http://192.168.184.71:8888/tree',
            icon: 'fa fa-line-chart',
            }
            ]
      
      
          },
          {
          name: 'Analytics Marketplace',
          url: '/file-manager',
          icon: 'fa fa-line-chart',
          }
    ]
  },

{
  name: 'Admin',
  url: '#',
  icon: 'fa fa-user-o',
  children: 
  [
    {
      name: 'User Management',
      url: '#',
      icon: 'fa fa-user-o',
      children: 
      [{
        name: 'Role Mapping',
          url: '/create-role',
          icon: 'fa fa-user-o',
      },
    {
      name: 'User Mapping',
          url: '/create-user',
          icon: 'fa fa-user-o',
    }]
    
    }
  ]

},



];