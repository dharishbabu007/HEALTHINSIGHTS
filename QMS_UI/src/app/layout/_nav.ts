export const navItems = [
  {
    name: 'Quality Management',
    url: '#',
    icon: 'fa fa-line-chart',
    children: [
      {
        name: 'Quality Central',
        url: '/quality-central',
        icon: 'fa fa-line-chart'
      },
      {
        name: 'Quality Program',
        url: 'http://healthinsight:8082/curis/hleft.jsp#!/',
        icon: 'fa fa-line-chart',
        children: [
          {
            name: 'MIPS',
            url: 'http://healthinsight:8082/curis/MIPS.html',
            icon: 'fa fa-line-chart'
          },
          {
            name: 'HEDIS',
            url: 'http://192.168.184.70/views/CurisDashboard_HEDIS_1/HEDIS_Plan?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
            icon: 'fa fa-line-chart'
          },
          {
            name: 'OHA',
            url: '#',
            icon: 'fa fa-line-chart'
          },
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
        url: 'http://192.168.184.70/views/Gaps_in_Care_Dashboard/CareGapSummaryDashboard?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
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
    ]
  },
  {
    name: 'PHM',
    url: '#',
    icon: 'fa fa-user-o',
    children: [
      {
        name: 'PHM SUMMARY',
        url: 'http://192.168.184.70/views/PHMsummary/PHMPerformanceSummaryDashboard?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
        icon: 'fa fa-user-o'
      },
      {
        name: 'Risk Assessment',
        url: 'http://192.168.184.70/views/PHMRiskDashboard_0/PHMRisk_Dashboard?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
        icon: 'fa fa-user-o'
      },
      {
        name: 'Measure Analysis',
        url: 'http://192.168.184.70/views/PHMMeasuressDashboard/PHMMeasures?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
        icon: 'fa fa-user-o'
      },
      {
        name: 'Membership Analysis',
        url: '#',
        icon: 'fa fa-user-o'
      },
      {
        name: 'Geo Analysis',
        url: 'http://192.168.184.70/views/Location_Dashboard_0/PHMLocationDashboard?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
        icon: 'fa fa-user-o'
      },
      {
        name: 'Cohort',
        url: 'http://192.168.184.70/views/FinalDashboardPHM/Cohort?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no',
        icon: 'fa fa-user-o'
      },
      {
        name: 'Utilitzation Analysis',
        url: '#',
        icon: 'fa fa-user-o'
      },
      {
        name: 'Financial Analysis',
        url: '#',
        icon: 'fa fa-user-o'
      },
      {
        name: 'Provider Analysis',
        url: '#',
        icon: 'fa fa-user-o'
      },
    ]
  }
];
