/*
 * ET Product Comparison – Split-Panel Viewer with Interactive Charts
 * ==================================================================
 * Side-by-side comparison of any two ET products on a split map.
 * Click anywhere on the map to see monthly and annual ET bar/line charts.
 *
 * ET Products:
 *   - USGS BCM/Flint (projects/nwi-usgs/assets/USGS-BCM-Flint-ET) [https://doi.org/10.3133/tm6H1; https://doi.org/10.5066/P9PT36UI]
 *   - USGS Reitz Ensemble (projects/nwi-usgs/assets/USGS-Reitz-Ensemble-ET) [https://doi.org/10.1029/2022WR034012; https://doi.org/10.5066/P9EZ3VAS]
 *   - USGS Reitz SSEBop-WB (projects/nwi-usgs/assets/USGS-Reitz-SSEBop-WB) [https://doi.org/10.3390/rs9121181; https://doi.org/10.5066/F7QC02FK]
 *   - MOD16 (MODIS/061/MOD16A2GF) [https://developers.google.com/earth-engine/datasets/catalog/MODIS_061_MOD16A2GF#description]
 *   - PMLv2 (projects/pml_evapotranspiration/PML/OUTPUT/PML_V22a) [https://developers.google.com/earth-engine/datasets/catalog/projects_pml_evapotranspiration_PML_OUTPUT_PML_V22a#description]
 *   - SSEBop VIIRS (projects/usgs-ssebop/viirs_et_v6_monthly) [https://gee-community-catalog.org/projects/usgs_viirs/]
 *   - SSEBop MODIS (projects/usgs-ssebop/modis_et_v5_monthly) [https://gee-community-catalog.org/projects/usgs_modis_et/]
 *   - OpenET Ensemble (OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0) [https://developers.google.com/earth-engine/datasets/catalog/OpenET_ENSEMBLE_CONUS_GRIDMET_MONTHLY_v2_0]
 *   - OpenET SSEBop (OpenET/SSEBOP/CONUS/GRIDMET/MONTHLY/v2_0) [https://developers.google.com/earth-engine/datasets/catalog/OpenET_SSEBOP_CONUS_GRIDMET_MONTHLY_v2_0]
 *   - OpenET eeMETRIC (OpenET/EEMETRIC/CONUS/GRIDMET/MONTHLY/v2_0) [https://developers.google.com/earth-engine/datasets/catalog/OpenET_EEMETRIC_CONUS_GRIDMET_MONTHLY_v2_0]
 *   - OpenET DisALEXI (OpenET/DISALEXI/CONUS/GRIDMET/MONTHLY/v2_0) [https://developers.google.com/earth-engine/datasets/catalog/OpenET_DISALEXI_CONUS_GRIDMET_MONTHLY_v2_0]
 *   - OpenET geeSEBAL (OpenET/GEESEBAL/CONUS/GRIDMET/MONTHLY/v2_0) [https://developers.google.com/earth-engine/datasets/catalog/OpenET_GEESEBAL_CONUS_GRIDMET_MONTHLY_v2_0]
 *   - OpenET PT-JPL (OpenET/PTJPL/CONUS/GRIDMET/MONTHLY/v2_0) [https://developers.google.com/earth-engine/datasets/catalog/OpenET_PTJPL_CONUS_GRIDMET_MONTHLY_v2_0]
 *   - OpenET SIMS (OpenET/SIMS/CONUS/GRIDMET/MONTHLY/v2_0) [https://developers.google.com/earth-engine/datasets/catalog/OpenET_SIMS_CONUS_GRIDMET_MONTHLY_v2_0]
 *   - WLDAS ET (projects/climate-engine-pro/assets/ce-wldas/daily) [https://support.climateengine.org/article/137-wldas]
 *   - TerraClimate (IDAHO_EPSCOR/TERRACLIMATE) [https://developers.google.com/earth-engine/datasets/catalog/IDAHO_EPSCOR_TERRACLIMATE#description]
 */

// ───────────────────────────────────────────────────────────────────
//  1. ET Product Definitions
// ───────────────────────────────────────────────────────────────────

var ET_PRODUCTS = {
  'USGS BCM/Flint': {
    collection: 'projects/nwi-usgs/assets/USGS-BCM-Flint-ET',
    band: 'b1',
    scale: 1,           // mm/yr (annual totals, one image per water year)
    monthly: false,
    annual: true,       // Annual product — do NOT sum, use directly
    startYear: 1896,
    endYear: 2024,
    nativeRes: 270
  },
  'USGS Reitz Ensemble': {
    collection: 'projects/nwi-usgs/assets/USGS-Reitz-Ensemble-ET',
    band: 'b1',
    scale: 1,          // mm/day, multiply by days in month
    monthly: true,
    startYear: 1896,
    endYear: 2018,
    nativeRes: 800
  },
  'USGS Reitz SSEBop-WB': {
    collection: 'projects/nwi-usgs/assets/USGS-Reitz-SSEBop-WB',
    band: 'b1',
    scale: 1000,       // m/month → mm/month
    monthly: true,
    startYear: 2000,
    endYear: 2015,
    nativeRes: 1000
  },
  'MOD16': {
    collection: 'MODIS/061/MOD16A2GF',
    band: 'ET',
    scale: 0.1,        // scale factor 0.1
    monthly: false,     // 8-day composite
    startYear: 2001,
    endYear: 2024,
    nativeRes: 500
  },
  'PMLv2': {
    collection: 'projects/pml_evapotranspiration/PML/OUTPUT/PML_V22a',
    band: 'ET',         // total ET band
    scale: 0.08,        // scale factor 0.08 → mm/month (0.08 = 0.01 (stored ×100) × 8 (8-day composite))
    monthly: false,
    startYear: 2002,
    endYear: 2024,
    nativeRes: 500
  },
  'SSEBop VIIRS': {
    collection: 'projects/usgs-ssebop/viirs_et_v6_monthly',
    band: 'et',
    scale: 1,
    monthly: true,
    startYear: 2012,
    endYear: 2024,
    nativeRes: 1000
  },
  'SSEBop MODIS': {
    collection: 'projects/usgs-ssebop/modis_et_v5_monthly',
    band: 'et',
    scale: 1,
    monthly: true,
    startYear: 2003,
    endYear: 2024,
    nativeRes: 1000
  },
  'OpenET Ensemble': {
    collection: 'OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0',
    band: 'et_ensemble_mad',
    scale: 1,
    monthly: true,
    startYear: 2000,
    endYear: 2024,
    nativeRes: 30
  },
  'OpenET SSEBop': {
    collection: 'OpenET/SSEBOP/CONUS/GRIDMET/MONTHLY/v2_0',
    band: 'et',
    scale: 1,
    monthly: true,
    startYear: 2000,
    endYear: 2024,
    nativeRes: 30
  },
  'OpenET eeMETRIC': {
    collection: 'OpenET/EEMETRIC/CONUS/GRIDMET/MONTHLY/v2_0',
    band: 'et',
    scale: 1,
    monthly: true,
    startYear: 2000,
    endYear: 2024,
    nativeRes: 30
  },
  'OpenET DisALEXI': {
    collection: 'OpenET/DISALEXI/CONUS/GRIDMET/MONTHLY/v2_0',
    band: 'et',
    scale: 1,
    monthly: true,
    startYear: 2000,
    endYear: 2024,
    nativeRes: 30
  },
  'OpenET geeSEBAL': {
    collection: 'OpenET/GEESEBAL/CONUS/GRIDMET/MONTHLY/v2_0',
    band: 'et',
    scale: 1,
    monthly: true,
    startYear: 2000,
    endYear: 2024,
    nativeRes: 30
  },
  'OpenET PT-JPL': {
    collection: 'OpenET/PTJPL/CONUS/GRIDMET/MONTHLY/v2_0',
    band: 'et',
    scale: 1,
    monthly: true,
    startYear: 2000,
    endYear: 2024,
    nativeRes: 30
  },
  'OpenET SIMS': {
    collection: 'OpenET/SIMS/CONUS/GRIDMET/MONTHLY/v2_0',
    band: 'et',
    scale: 1,
    monthly: true,
    startYear: 2000,
    endYear: 2024,
    nativeRes: 30
  },
  'WLDAS': {
    collection: 'projects/climate-engine-pro/assets/ce-wldas/daily',
    band: 'Evap_tavg',
    scale: 86400,   // kg/m²/s → mm/day
    monthly: false, // daily
    startYear: 1979,
    endYear: 2024,
    nativeRes: 1000
  },
  'TerraClimate': {
    collection: 'IDAHO_EPSCOR/TERRACLIMATE',
    band: 'aet',
    scale: 0.1,
    monthly: true,
    startYear: 1958,
    endYear: 2024,
    nativeRes: 4638
  }
};

var productNames = Object.keys(ET_PRODUCTS);

// ───────────────────────────────────────────────────────────────────
//  2. Visualization Parameters
// ───────────────────────────────────────────────────────────────────

var ET_VIS = {
  min: 0,
  max: 300,
  palette: [
    'ffffcc', 'c7e9b4', '7fcdbb', '41b6c4',
    '1d91c0', '225ea8', '253494', '081d58'
  ]
};

// ───────────────────────────────────────────────────────────────────
//  3. Helper: Get Monthly ET Images for a Year
// ───────────────────────────────────────────────────────────────────

/**
 * Returns a list of monthly ee.Image objects (ET in mm/month) for
 * the given product and year.
 */
function getMonthlyET(productName, year) {
  var info = ET_PRODUCTS[productName];
  var col = ee.ImageCollection(info.collection);

  // Annual-only products (e.g. USGS BCM/Flint): return the single annual
  // image directly — no monthly breakdown is available.
  if (info.annual) {
    var wyStart = ee.Date.fromYMD(ee.Number(year).subtract(1), 10, 1);
    var wyEnd = wyStart.advance(1, 'year');
    var annualImg = col.filterDate(wyStart, wyEnd)
      .select(info.band).first();
    var emptyBand = info.band || 'b1';
    var emptyImg = ee.Image.constant(0).rename(emptyBand)
      .updateMask(ee.Image.constant(0));
    annualImg = ee.Image(ee.Algorithms.If(
      col.filterDate(wyStart, wyEnd).size().gt(0),
      annualImg.multiply(info.scale),
      emptyImg
    ));
    return ee.ImageCollection([annualImg.toFloat()
      .set('year', year)
      .set('month', 1)
      .set('system:time_start', wyStart.millis())]);
  }

  var months = ee.List.sequence(1, 12);

  var monthlyImages = months.map(function (m) {
    m = ee.Number(m);
    var start = ee.Date.fromYMD(year, m, 1);
    var end = start.advance(1, 'month');
    var filtered = col.filterDate(start, end);

    // Fallback: fully-masked zero image when collection is empty
    var emptyBand = info.band || 'b1';
    var emptyImg = ee.Image.constant(0).rename(emptyBand)
      .updateMask(ee.Image.constant(0));
    var count = filtered.size();

    var monthET;
    if (productName === 'USGS Reitz Ensemble') {
      // mm/day → mm/month: multiply by number of days. The source raster
      // masks truly-dry pixels (e.g. playas), so unmask(0) treats them as
      // zero ET instead of nodata, matching the local 200 m rasters.
      var daysInMonth = end.difference(start, 'day');
      monthET = ee.Image(ee.Algorithms.If(
        count.gt(0),
        filtered.select(info.band).mean().multiply(daysInMonth).unmask(0),
        emptyImg
      ));
    } else if (productName === 'WLDAS') {
      // Daily: Evap_tavg in kg/m²/s, ×86400 → mm/day, sum days in month
      monthET = ee.Image(ee.Algorithms.If(
        count.gt(0),
        filtered.select(info.band).sum().multiply(info.scale),
        emptyImg
      ));
    } else if (productName === 'USGS Reitz SSEBop-WB') {
      // m/month → mm/month. The source raster masks truly-dry pixels
      // (e.g. playas), so unmask(0) treats them as zero ET instead of
      // nodata, matching the local 200 m rasters.
      monthET = ee.Image(ee.Algorithms.If(
        count.gt(0),
        filtered.select(info.band).sum().multiply(info.scale).unmask(0),
        emptyImg
      ));
    } else {
      monthET = ee.Image(ee.Algorithms.If(
        count.gt(0),
        filtered.select(info.band).sum().multiply(info.scale),
        emptyImg
      ));
    }

    return monthET.toFloat()
      .set('year', year)
      .set('month', m)
      .set('system:time_start', start.millis());
  });

  return ee.ImageCollection.fromImages(monthlyImages);
}

/**
 * Returns a mean annual ET image (mm/yr) averaged over startYear–endYear.
 */
function getMeanAnnualET(productName, startYear, endYear) {
  var years = ee.List.sequence(startYear, endYear);
  var annualImages = years.map(function (y) {
    y = ee.Number(y).toInt();
    var monthly = getMonthlyET(productName, y);
    return monthly.sum().set('year', y);
  });
  return ee.ImageCollection.fromImages(annualImages).mean();
}

// ───────────────────────────────────────────────────────────────────
//  4. UI: Build Controls
// ───────────────────────────────────────────────────────────────────

ui.root.clear();

// Default selections
var leftProduct = 'USGS Reitz SSEBop-WB';
var rightProduct = 'USGS Reitz Ensemble';
var chartYear = 2015;
var mapMode = 'mean';  // 'mean' or 'annual'
var chartUnits = 'imperial';  // 'mm' or 'imperial' (inches/ft)
var meanStartYear = 2001;
var meanEndYear = 2015;
var visMin = 0;
var visMax = 300;

// Conversion factors: 1 mm = 0.0393701 in, 1 mm = 0.00328084 ft
var MM_TO_INCH = 0.0393701;
var MM_TO_FT = 0.00328084;

// Determine overlapping year range for defaults
function overlapRange(a, b) {
  var infoA = ET_PRODUCTS[a], infoB = ET_PRODUCTS[b];
  return {
    start: Math.max(infoA.startYear, infoB.startYear),
    end: Math.min(infoA.endYear, infoB.endYear)
  };
}

// --- Dropdowns ---
var leftSelect = ui.Select({
  items: productNames,
  value: leftProduct,
  style: {stretch: 'horizontal'},
  onChange: function (val) { leftProduct = val; refreshMap(); }
});

var rightSelect = ui.Select({
  items: productNames,
  value: rightProduct,
  style: {stretch: 'horizontal'},
  onChange: function (val) { rightProduct = val; refreshMap(); }
});

// --- Map mode selector ---
var mapModeSelect = ui.Select({
  items: [
    {label: 'Mean Annual (long-term)', value: 'mean'},
    {label: 'Single Year', value: 'annual'}
  ],
  value: mapMode,
  style: {stretch: 'horizontal'},
  onChange: function (val) { mapMode = val; refreshMap(); }
});

// --- Chart mode selector (pixel vs basin) ---
var chartMode = 'pixel';  // 'pixel' or 'basin'
var basinSet = 'subbasin'; // 'calibration' or 'subbasin'

var chartModeSelect = ui.Select({
  items: [
    {label: 'Pixel (single point)', value: 'pixel'},
    {label: 'Basin (spatial mean)', value: 'basin'}
  ],
  value: chartMode,
  style: {stretch: 'horizontal'},
  onChange: function (val) { chartMode = val; }
});

var basinSetSelect = ui.Select({
  items: [
    {label: 'Sub-Basins (HU-10)', value: 'subbasin'},
    {label: 'Calibration Basins (HU-12)', value: 'calibration'}
  ],
  value: basinSet,
  style: {stretch: 'horizontal'},
  onChange: function (val) { basinSet = val; }
});

// --- Chart units selector ---
var unitSelect = ui.Select({
  items: [
    {label: 'Metric (mm / mm)', value: 'mm'},
    {label: 'Imperial (inches / ft)', value: 'imperial'}
  ],
  value: chartUnits,
  style: {stretch: 'horizontal'},
  onChange: function (val) { chartUnits = val; }
});

// --- Year slider ---
var yearSlider = ui.Slider({
  min: 1896, max: 2024, value: chartYear, step: 1,
  style: {stretch: 'horizontal'},
  onChange: function (val) {
    chartYear = Math.round(val);
    if (mapMode === 'annual') { refreshMap(); }
  }
});

// --- Mean annual year range sliders ---
var meanStartSlider = ui.Slider({
  min: 1896, max: 2024, value: meanStartYear, step: 1,
  style: {stretch: 'horizontal'},
  onChange: function (val) { meanStartYear = Math.round(val); }
});

var meanEndSlider = ui.Slider({
  min: 1896, max: 2024, value: meanEndYear, step: 1,
  style: {stretch: 'horizontal'},
  onChange: function (val) { meanEndYear = Math.round(val); }
});

// --- Colorbar min/max sliders ---
var visMinSlider = ui.Slider({
  min: 0, max: 2000, value: visMin, step: 10,
  style: {stretch: 'horizontal'},
  onChange: function (val) { visMin = Math.round(val); }
});

var visMaxSlider = ui.Slider({
  min: 0, max: 2000, value: visMax, step: 10,
  style: {stretch: 'horizontal'},
  onChange: function (val) { visMax = Math.round(val); }
});

// ───────────────────────────────────────────────────────────────────
//  5. Build Split-Panel Map
// ───────────────────────────────────────────────────────────────────

var leftMap = ui.Map();
var rightMap = ui.Map();

leftMap.setControlVisibility({mapTypeControl: true, layerList: false});
rightMap.setControlVisibility({mapTypeControl: true, layerList: false});

// Default basemap: satellite-with-labels (Google hybrid). Other options:
//   'roadmap', 'terrain', 'satellite', 'hybrid'
leftMap.setOptions('HYBRID');
rightMap.setOptions('HYBRID');

var splitPanel = ui.SplitPanel({
  firstPanel: leftMap,
  secondPanel: rightMap,
  orientation: 'horizontal',
  wipe: true
});

var linker = ui.Map.Linker([leftMap, rightMap]);

// ───────────────────────────────────────────────────────────────────
//  5b. Indian Wells Valley (IWV) basin outline
// ───────────────────────────────────────────────────────────────────
// Single dissolved polygon of the IWV sub-basins (WGS84), simplified to
// ~200 m tolerance. Used to frame the map and overlay the basin boundary.
var IWV_BASIN_COORDS = [[[-118.08809,35.46053],[-118.10305,35.47167],[-118.10905,35.48175],[-118.13318,35.49606],[-118.13398,35.50778],[-118.12495,35.51085],[-118.12498,35.51801],[-118.12805,35.52963],[-118.13324,35.53507],[-118.1443,35.53897],[-118.14471,35.54437],[-118.14066,35.55035],[-118.13369,35.55226],[-118.13323,35.55709],[-118.12664,35.56354],[-118.12828,35.57337],[-118.12472,35.58141],[-118.115,35.58579],[-118.11347,35.59164],[-118.11973,35.60705],[-118.11333,35.61625],[-118.09856,35.6215],[-118.08925,35.62075],[-118.08748,35.62426],[-118.09018,35.62533],[-118.09127,35.63526],[-118.08212,35.63006],[-118.07306,35.62971],[-118.07026,35.63495],[-118.06145,35.6361],[-118.05485,35.63159],[-118.04185,35.63382],[-118.03224,35.63061],[-118.0363,35.65578],[-118.03248,35.66068],[-118.0276,35.66137],[-118.01535,35.67873],[-118.00914,35.67691],[-118.00318,35.68531],[-117.99524,35.68523],[-117.98592,35.69288],[-117.98586,35.7012],[-117.99214,35.70549],[-117.99552,35.71685],[-118.00362,35.72329],[-117.99696,35.73807],[-118.01166,35.75561],[-118.00099,35.77162],[-118.00627,35.77717],[-118.00128,35.78237],[-118.00709,35.78795],[-118.00964,35.79511],[-118.00753,35.799],[-118.01127,35.80405],[-118.00767,35.81024],[-118.00844,35.81676],[-117.99875,35.82324],[-118.00579,35.83029],[-118.00198,35.84314],[-118.00636,35.84558],[-118.00807,35.86161],[-117.99657,35.8695],[-117.98879,35.8656],[-117.98082,35.86703],[-117.98342,35.8746],[-117.99053,35.8814],[-117.98346,35.89336],[-117.9891,35.90169],[-117.98614,35.90582],[-117.99049,35.90861],[-117.99177,35.91425],[-117.98422,35.92653],[-117.98679,35.93454],[-117.99287,35.94415],[-117.99998,35.94735],[-118.0055,35.94619],[-118.00984,35.94906],[-118.01055,35.95481],[-118.01724,35.95503],[-118.01843,35.96313],[-118.0122,35.96822],[-118.01475,35.97274],[-118.00964,35.97832],[-118.00351,35.97728],[-118.00403,35.98443],[-118.01074,35.99154],[-118.0125,35.99835],[-118.03145,36.00706],[-118.03567,36.01229],[-118.03727,36.02875],[-118.04225,36.03017],[-118.03944,36.03422],[-118.04695,36.04225],[-118.04186,36.04898],[-118.04869,36.0536],[-118.05181,36.06066],[-118.05181,36.08384],[-118.05716,36.09182],[-118.06702,36.0922],[-118.06825,36.10979],[-118.06526,36.11257],[-118.07386,36.14026],[-118.05963,36.15001],[-118.02359,36.13971],[-117.97276,36.14025],[-117.95254,36.13735],[-117.94539,36.13367],[-117.94547,36.12548],[-117.93707,36.11741],[-117.92403,36.1197],[-117.91456,36.12538],[-117.91434,36.13133],[-117.89835,36.13356],[-117.89077,36.12871],[-117.89458,36.11018],[-117.89077,36.11207],[-117.88779,36.10995],[-117.88593,36.11648],[-117.8679,36.11407],[-117.86289,36.11743],[-117.85206,36.13985],[-117.83963,36.14347],[-117.83809,36.1536],[-117.82525,36.15617],[-117.82009,36.15432],[-117.81196,36.16133],[-117.80417,36.16229],[-117.7958,36.17435],[-117.79655,36.18353],[-117.78863,36.18483],[-117.78535,36.1891],[-117.77403,36.18295],[-117.77369,36.17993],[-117.76279,36.17879],[-117.7575,36.1917],[-117.74787,36.19696],[-117.74092,36.19319],[-117.73457,36.19511],[-117.73295,36.20373],[-117.7194,36.20863],[-117.7134,36.20374],[-117.71853,36.1922],[-117.71366,36.18508],[-117.71721,36.17727],[-117.70458,36.17097],[-117.69076,36.17002],[-117.68645,36.16015],[-117.68728,36.14626],[-117.67664,36.13841],[-117.67605,36.12942],[-117.6685,36.11874],[-117.65649,36.11479],[-117.65291,36.11516],[-117.6508,36.11985],[-117.64402,36.12027],[-117.64154,36.11454],[-117.63369,36.11242],[-117.63248,36.10528],[-117.61987,36.0919],[-117.59792,36.08536],[-117.60359,36.07121],[-117.58834,36.06213],[-117.59508,36.04995],[-117.58727,36.05113],[-117.58569,36.05592],[-117.57537,36.05961],[-117.56504,36.05899],[-117.55617,36.06287],[-117.56003,36.06819],[-117.55646,36.07092],[-117.54026,36.06044],[-117.53829,36.05294],[-117.54189,36.04975],[-117.53904,36.04195],[-117.54768,36.04024],[-117.55542,36.02925],[-117.5478,36.02277],[-117.54581,36.01563],[-117.53109,36.0074],[-117.53078,36.00268],[-117.52276,35.99696],[-117.52412,35.98941],[-117.51619,35.98517],[-117.52362,35.9825],[-117.52321,35.97829],[-117.51671,35.97739],[-117.51644,35.97061],[-117.50808,35.96786],[-117.49967,35.95255],[-117.50385,35.94819],[-117.49409,35.94016],[-117.51208,35.92469],[-117.50349,35.91971],[-117.50273,35.90815],[-117.49192,35.90441],[-117.48571,35.90867],[-117.47903,35.90746],[-117.45899,35.89553],[-117.46143,35.88582],[-117.45763,35.87848],[-117.46163,35.87474],[-117.45997,35.85969],[-117.45476,35.85461],[-117.44584,35.85452],[-117.45223,35.84988],[-117.47023,35.8476],[-117.47544,35.84315],[-117.47023,35.84065],[-117.47048,35.83578],[-117.46356,35.82939],[-117.46329,35.82077],[-117.45261,35.81484],[-117.45286,35.8092],[-117.44909,35.80434],[-117.45705,35.80451],[-117.45534,35.791],[-117.46041,35.7879],[-117.47038,35.7932],[-117.4772,35.78897],[-117.47786,35.77321],[-117.46768,35.76661],[-117.45841,35.75528],[-117.46149,35.75186],[-117.43379,35.74828],[-117.44045,35.73443],[-117.44437,35.73303],[-117.43544,35.72585],[-117.43902,35.71971],[-117.43264,35.71657],[-117.43405,35.71256],[-117.43913,35.71409],[-117.44118,35.70637],[-117.43498,35.69547],[-117.42124,35.69439],[-117.40466,35.69823],[-117.40194,35.69346],[-117.39585,35.69546],[-117.39069,35.69325],[-117.37806,35.69611],[-117.37432,35.68825],[-117.3862,35.6748],[-117.40712,35.67169],[-117.41629,35.66575],[-117.41785,35.66066],[-117.40959,35.65117],[-117.42356,35.64056],[-117.43022,35.64174],[-117.44027,35.63495],[-117.45881,35.63938],[-117.46522,35.63369],[-117.469,35.62851],[-117.46756,35.62099],[-117.46487,35.61293],[-117.45807,35.60952],[-117.45941,35.60133],[-117.45195,35.59814],[-117.4539,35.59422],[-117.45927,35.59316],[-117.46373,35.59827],[-117.479,35.59179],[-117.48327,35.59524],[-117.49578,35.59697],[-117.5107,35.58782],[-117.51359,35.57852],[-117.52239,35.57857],[-117.5269,35.56933],[-117.54015,35.56285],[-117.55179,35.56773],[-117.55641,35.57323],[-117.5797,35.56336],[-117.58123,35.5546],[-117.58673,35.55468],[-117.59025,35.54976],[-117.61109,35.54929],[-117.61469,35.54435],[-117.63971,35.54168],[-117.64256,35.54019],[-117.6425,35.53451],[-117.65548,35.5365],[-117.65913,35.53176],[-117.65749,35.52496],[-117.67396,35.49084],[-117.68597,35.48431],[-117.6841,35.47994],[-117.69051,35.47648],[-117.69992,35.4782],[-117.7098,35.47538],[-117.70667,35.4704],[-117.70831,35.46485],[-117.71534,35.46223],[-117.71933,35.46527],[-117.72589,35.46325],[-117.74218,35.4653],[-117.74719,35.46379],[-117.74705,35.46033],[-117.75447,35.46469],[-117.76084,35.46336],[-117.77194,35.48292],[-117.79104,35.48717],[-117.7869,35.49443],[-117.78993,35.49458],[-117.79905,35.48747],[-117.80967,35.48754],[-117.81876,35.4809],[-117.83689,35.48243],[-117.84325,35.47331],[-117.85938,35.46431],[-117.87728,35.46264],[-117.88446,35.45522],[-117.8846,35.45117],[-117.89165,35.45181],[-117.89904,35.4439],[-117.90416,35.4483],[-117.90907,35.44644],[-117.91422,35.44912],[-117.91772,35.44578],[-117.92363,35.44607],[-117.92885,35.43524],[-117.93505,35.43661],[-117.94282,35.42776],[-117.96029,35.42532],[-117.96759,35.43025],[-118.00613,35.44224],[-118.02725,35.43845],[-118.04246,35.44211],[-118.06335,35.45878],[-118.08809,35.46053]]];

var iwvBasin = ee.Geometry.Polygon(IWV_BASIN_COORDS, null, false);
var iwvOutline = ee.Image().byte()
  .paint({featureCollection: ee.FeatureCollection([ee.Feature(iwvBasin)]),
          color: 1, width: 2});
var iwvOutlineVis = {palette: ['000000']};
leftMap.addLayer(iwvOutline, iwvOutlineVis, 'IWV basin');
rightMap.addLayer(iwvOutline, iwvOutlineVis, 'IWV basin');
leftMap.centerObject(iwvBasin, 9);

// ───────────────────────────────────────────────────────────────────
//  5c. Calibration basins and sub-basins (GEE assets)
// ───────────────────────────────────────────────────────────────────
// Uploaded from Saleh & Flint (2019): https://doi.org/10.5066/F7JM28XZ

var calibrationBasins = ee.FeatureCollection(
  'projects/nwi-usgs/assets/IWV_calibrationBasin/IWV_calibrationBasin');
var subBasins = ee.FeatureCollection(
  'projects/nwi-usgs/assets/IWV_SubBasin/IWV_SubBasin');

// Basin masks: sub-basins always shown, calibration basins only when toggled
var subBasinMask = ee.Image.constant(1).clip(subBasins).mask();
var calBasinMask = ee.Image.constant(1).clip(calibrationBasins).mask();

// Paint outlines and labels
var calibrationOutline = ee.Image().byte()
  .paint({featureCollection: calibrationBasins, color: 1, width: 2});
var subBasinOutline = ee.Image().byte()
  .paint({featureCollection: subBasins, color: 1, width: 2});

// Visibility state
var showCalibration = false;
var showSubBasins = false;

// Layer references (set in addBasinLayers)
var leftCalLayer, rightCalLayer, leftSubLayer, rightSubLayer;
var leftCalLabels, rightCalLabels, leftSubLabels, rightSubLabels;

function addBasinLayers() {
  // Calibration basins (red)
  leftCalLayer = ui.Map.Layer(
    calibrationOutline, {palette: ['FF0000']}, 'Calibration Basins', showCalibration);
  rightCalLayer = ui.Map.Layer(
    calibrationOutline, {palette: ['FF0000']}, 'Calibration Basins', showCalibration);
  leftMap.layers().add(leftCalLayer);
  rightMap.layers().add(rightCalLayer);

  // Calibration basin labels
  leftCalLabels = ui.Map.Layer(
    calibrationBasins.style({color: '00000000', fillColor: '00000000', width: 0}),
    {}, 'Calibration Labels', showCalibration);
  rightCalLabels = ui.Map.Layer(
    calibrationBasins.style({color: '00000000', fillColor: '00000000', width: 0}),
    {}, 'Calibration Labels', showCalibration);
  leftMap.layers().add(leftCalLabels);
  rightMap.layers().add(rightCalLabels);

  // Sub-basins (black)
  leftSubLayer = ui.Map.Layer(
    subBasinOutline, {palette: ['000000']}, 'Sub-Basins', showSubBasins);
  rightSubLayer = ui.Map.Layer(
    subBasinOutline, {palette: ['000000']}, 'Sub-Basins', showSubBasins);
  leftMap.layers().add(leftSubLayer);
  rightMap.layers().add(rightSubLayer);

  // Sub-basin labels
  leftSubLabels = ui.Map.Layer(
    subBasins.style({color: '00000000', fillColor: '00000000', width: 0}),
    {}, 'Sub-Basin Labels', showSubBasins);
  rightSubLabels = ui.Map.Layer(
    subBasins.style({color: '00000000', fillColor: '00000000', width: 0}),
    {}, 'Sub-Basin Labels', showSubBasins);
  leftMap.layers().add(leftSubLabels);
  rightMap.layers().add(rightSubLabels);
}

addBasinLayers();

// ───────────────────────────────────────────────────────────────────
//  6. Chart Panel (bottom)
// ───────────────────────────────────────────────────────────────────

var chartPanel = ui.Panel({
  style: {
    height: '340px',
    stretch: 'horizontal',
    position: 'bottom-center'
  }
});

var chartPlaceholder = ui.Label(
  '🖱 Click on the map to generate monthly and annual ET charts',
  {fontSize: '14px', color: 'gray', textAlign: 'center', stretch: 'horizontal'}
);
chartPanel.add(chartPlaceholder);

// ───────────────────────────────────────────────────────────────────
//  7. Control Panel (left sidebar)
// ───────────────────────────────────────────────────────────────────

var controlPanel = ui.Panel({
  style: {width: '320px', padding: '10px'}
});

controlPanel.add(ui.Label('ET Product Comparison', {
  fontWeight: 'bold', fontSize: '18px', margin: '0 0 10px 0'
}));

// About section with product info and hyperlinks
var aboutIntro = ui.Label(
  'Side-by-side comparison of ET products on a split map. ' +
  'Click anywhere to see monthly and annual ET charts.',
  {fontSize: '11px', color: '#444', margin: '0 0 6px 0'}
);

var aboutProductsHeader = ui.Label('Products:', {fontSize: '11px', fontWeight: 'bold', margin: '0 0 4px 0'});

// Helper to create linked product labels
function productLink(name, url) {
  return ui.Label(name, {fontSize: '10px', color: '#1a73e8', margin: '0 0 2px 8px'}, url);
}

var productLinks = [
  productLink('• USGS BCM/Flint (annual, 270 m)', 'https://doi.org/10.5066/P9PT36UI'),
  productLink('• USGS Reitz Ensemble', 'https://doi.org/10.1029/2022WR034012'),
  productLink('• USGS Reitz SSEBop-WB', 'https://doi.org/10.3390/rs9121181'),
  productLink('• MOD16', 'https://developers.google.com/earth-engine/datasets/catalog/MODIS_061_MOD16A2GF'),
  productLink('• PMLv2', 'https://developers.google.com/earth-engine/datasets/catalog/projects_pml_evapotranspiration_PML_OUTPUT_PML_V22a'),
  productLink('• SSEBop VIIRS', 'https://gee-community-catalog.org/projects/usgs_viirs/'),
  productLink('• SSEBop MODIS', 'https://gee-community-catalog.org/projects/usgs_modis_et/'),
  productLink('• OpenET Ensemble', 'https://developers.google.com/earth-engine/datasets/catalog/OpenET_ENSEMBLE_CONUS_GRIDMET_MONTHLY_v2_0'),
  productLink('• OpenET SSEBop', 'https://developers.google.com/earth-engine/datasets/catalog/OpenET_SSEBOP_CONUS_GRIDMET_MONTHLY_v2_0'),
  productLink('• OpenET eeMETRIC', 'https://developers.google.com/earth-engine/datasets/catalog/OpenET_EEMETRIC_CONUS_GRIDMET_MONTHLY_v2_0'),
  productLink('• OpenET DisALEXI', 'https://developers.google.com/earth-engine/datasets/catalog/OpenET_DISALEXI_CONUS_GRIDMET_MONTHLY_v2_0'),
  productLink('• OpenET geeSEBAL', 'https://developers.google.com/earth-engine/datasets/catalog/OpenET_GEESEBAL_CONUS_GRIDMET_MONTHLY_v2_0'),
  productLink('• OpenET PT-JPL', 'https://developers.google.com/earth-engine/datasets/catalog/OpenET_PTJPL_CONUS_GRIDMET_MONTHLY_v2_0'),
  productLink('• OpenET SIMS', 'https://developers.google.com/earth-engine/datasets/catalog/OpenET_SIMS_CONUS_GRIDMET_MONTHLY_v2_0'),
  productLink('• WLDAS ET', 'https://support.climateengine.org/article/137-wldas'),
  productLink('• TerraClimate', 'https://developers.google.com/earth-engine/datasets/catalog/IDAHO_EPSCOR_TERRACLIMATE')
];

var aboutPanel = ui.Panel({
  widgets: [aboutIntro, aboutProductsHeader].concat(productLinks),
  style: {margin: '0 0 10px 0', shown: false}
});

var aboutToggle = ui.Button({
  label: 'ℹ️ About',
  style: {stretch: 'horizontal', margin: '0 0 5px 0'},
  onClick: function() {
    aboutPanel.style().set('shown', !aboutPanel.style().get('shown'));
  }
});

controlPanel.add(aboutToggle);
controlPanel.add(aboutPanel);

controlPanel.add(ui.Label('Left Panel:', {fontWeight: 'bold'}));
controlPanel.add(leftSelect);

controlPanel.add(ui.Label('Right Panel:', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(rightSelect);

controlPanel.add(ui.Label('Map Mode:', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(mapModeSelect);

controlPanel.add(ui.Label('Chart Mode:', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(chartModeSelect);
controlPanel.add(basinSetSelect);

controlPanel.add(ui.Label('Chart Units:', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(unitSelect);

controlPanel.add(ui.Label('Year (charts & single-year map):', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(yearSlider);

controlPanel.add(ui.Label('Mean Annual Start Year:', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(meanStartSlider);

controlPanel.add(ui.Label('Mean Annual End Year:', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(meanEndSlider);

controlPanel.add(ui.Label('Colorbar Min (mm/yr):', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(visMinSlider);

controlPanel.add(ui.Label('Colorbar Max (mm/yr):', {fontWeight: 'bold', margin: '10px 0 0 0'}));
controlPanel.add(visMaxSlider);

var refreshButton = ui.Button({
  label: '🔄 Refresh Map Layers',
  style: {stretch: 'horizontal', margin: '10px 0'},
  onClick: function () { refreshMap(); }
});
controlPanel.add(refreshButton);

// Basin overlay toggles
controlPanel.add(ui.Label('Basin Overlays:', {fontWeight: 'bold', margin: '10px 0 0 0'}));

var calCheckbox = ui.Checkbox({
  label: 'IWV BCM Calibration Basins (red)',
  value: showCalibration,
  style: {fontSize: '12px', margin: '2px 0'},
  onChange: function (checked) {
    showCalibration = checked;
    refreshMap();  // Recreates all layers including ET mask and basin overlays
  }
});
controlPanel.add(calCheckbox);

var subCheckbox = ui.Checkbox({
  label: 'IWV Sub-Basins (black)',
  value: showSubBasins,
  style: {fontSize: '12px', margin: '2px 0'},
  onChange: function (checked) {
    showSubBasins = checked;
    refreshMap();  // Recreates all layers including basin overlays
  }
});
controlPanel.add(subCheckbox);

controlPanel.add(ui.Label(
  'Click the map to see monthly & annual ET at that point.',
  {fontSize: '12px', color: '#555', margin: '10px 0 0 0'}
));

// Legend
var legendLabel = ui.Label('ET (mm/yr)', {
  fontWeight: 'bold', margin: '20px 0 4px 0'
});
controlPanel.add(legendLabel);

var legendPanel = ui.Panel();
function updateLegend() {
  legendPanel.clear();
  legendPanel.add(makeLegend({min: visMin, max: visMax, palette: ET_VIS.palette}));
}

function makeLegend(vis) {
  var legend = ui.Panel({style: {margin: '0 0 0 0'}});
  // Continuous gradient bar
  var gradient = ui.Thumbnail({
    image: ee.Image.pixelLonLat().select('longitude')
      .multiply((vis.max - vis.min) / 100).add(vis.min),
    params: {
      bbox: [0, 0, 100, 1],
      dimensions: '250x15',
      format: 'png',
      min: vis.min,
      max: vis.max,
      palette: vis.palette
    },
    style: {stretch: 'horizontal', margin: '0 0 4px 0'}
  });
  legend.add(gradient);
  // Min / Max labels
  var labels = ui.Panel({
    layout: ui.Panel.Layout.flow('horizontal'),
    style: {stretch: 'horizontal'}
  });
  labels.add(ui.Label(String(vis.min), {fontSize: '11px', margin: '0'}));
  labels.add(ui.Label(String(vis.max), {
    fontSize: '11px', margin: '0', textAlign: 'right', stretch: 'horizontal'
  }));
  legend.add(labels);
  return legend;
}
controlPanel.add(legendPanel);
updateLegend();

// ───────────────────────────────────────────────────────────────────
//  8. Layout Assembly
// ───────────────────────────────────────────────────────────────────

var mapAndChart = ui.Panel({
  widgets: [splitPanel, chartPanel],
  layout: ui.Panel.Layout.flow('vertical'),
  style: {stretch: 'both'}
});

var mainPanel = ui.SplitPanel({
  firstPanel: controlPanel,
  secondPanel: mapAndChart,
  orientation: 'horizontal'
});

ui.root.add(mainPanel);

// ───────────────────────────────────────────────────────────────────
//  9. Refresh Map Layers
// ───────────────────────────────────────────────────────────────────

/**
 * Returns a single-year annual ET image (mm/yr) for the given product and year.
 */
function getAnnualET(productName, year) {
  return getMonthlyET(productName, year).sum();
}

function refreshMap() {
  leftMap.layers().reset();
  rightMap.layers().reset();

  // Update vis params from current slider values
  var currentVis = {
    min: visMin,
    max: visMax,
    palette: ET_VIS.palette
  };
  updateLegend();

  var leftImage, rightImage, leftLabel, rightLabel;

  if (mapMode === 'annual') {
    // Single-year comparison
    var y = chartYear;
    leftImage = getAnnualET(leftProduct, y);
    rightImage = getAnnualET(rightProduct, y);
    leftLabel = leftProduct + ' (' + y + ')';
    rightLabel = rightProduct + ' (' + y + ')';
  } else {
    // Mean annual (long-term) — use custom year range, clamped to each product's availability
    var ls = Math.max(meanStartYear, ET_PRODUCTS[leftProduct].startYear);
    var le = Math.min(meanEndYear, ET_PRODUCTS[leftProduct].endYear);
    var rs = Math.max(meanStartYear, ET_PRODUCTS[rightProduct].startYear);
    var re = Math.min(meanEndYear, ET_PRODUCTS[rightProduct].endYear);
    if (ls > le) { ls = ET_PRODUCTS[leftProduct].startYear; le = ET_PRODUCTS[leftProduct].endYear; }
    if (rs > re) { rs = ET_PRODUCTS[rightProduct].startYear; re = ET_PRODUCTS[rightProduct].endYear; }
    leftImage = getMeanAnnualET(leftProduct, ls, le);
    rightImage = getMeanAnnualET(rightProduct, rs, re);
    leftLabel = leftProduct + ' (mean ' + ls + '–' + le + ')';
    rightLabel = rightProduct + ' (mean ' + rs + '–' + re + ')';
  }

  // Mask ET images: sub-basins always, calibration basins only when toggled
  var activeMask = showCalibration
    ? subBasinMask.max(calBasinMask)
    : subBasinMask;
  leftMap.addLayer(leftImage.updateMask(activeMask), currentVis, leftProduct);
  rightMap.addLayer(rightImage.updateMask(activeMask), currentVis, rightProduct);

  // Re-add IWV basin outline on top
  leftMap.addLayer(iwvOutline, iwvOutlineVis, 'IWV basin');
  rightMap.addLayer(iwvOutline, iwvOutlineVis, 'IWV basin');

  // Re-add basin overlay layers (preserving toggle state)
  addBasinLayers();

  // Add labels
  leftMap.widgets().reset();
  rightMap.widgets().reset();
  leftMap.widgets().add(ui.Label(leftLabel, {
    fontWeight: 'bold', fontSize: '14px', backgroundColor: 'rgba(255,255,255,0.8)',
    padding: '4px 8px', position: 'top-left'
  }));
  rightMap.widgets().add(ui.Label(rightLabel, {
    fontWeight: 'bold', fontSize: '14px', backgroundColor: 'rgba(255,255,255,0.8)',
    padding: '4px 8px', position: 'top-right'
  }));
}

// ───────────────────────────────────────────────────────────────────
// 10. Click-to-Chart
// ───────────────────────────────────────────────────────────────────

function onMapClick(coords) {
  chartPanel.clear();
  chartPanel.add(ui.Label('⏳ Loading charts...', {
    fontSize: '14px', color: 'gray', textAlign: 'center', stretch: 'horizontal'
  }));

  var point = ee.Geometry.Point([coords.lon, coords.lat]);

  // In basin mode, resolve the basin geometry first, then build charts
  if (chartMode === 'basin') {
    var fc = (basinSet === 'calibration') ? calibrationBasins : subBasins;
    var nameCol = (basinSet === 'calibration') ? 'HU_12_NAME' : 'HU_10_NAME';
    var hit = fc.filterBounds(point);
    hit.size().evaluate(function (count) {
      if (count === 0) {
        chartPanel.clear();
        chartPanel.add(ui.Label(
          'Clicked point is outside ' + (basinSet === 'calibration'
            ? 'calibration' : 'sub-') + 'basin boundaries.',
          {fontSize: '14px', color: 'gray', textAlign: 'center', stretch: 'horizontal'}
        ));
        return;
      }
      var basinFeature = hit.first();
      var basinGeom = basinFeature.geometry();
      basinFeature.get(nameCol).evaluate(function (bName) {
        var locLabel = bName + ' (' + (basinSet === 'calibration'
          ? 'calibration' : 'sub-basin') + ')';
        _buildCharts(coords, basinGeom, locLabel, true);
      });
    });
  } else {
    // Pixel mode
    var locLabel = coords.lat.toFixed(3) + '°N, ' + coords.lon.toFixed(3) + '°E';
    _buildCharts(coords, point, locLabel, false);
  }
}

/**
 * Core chart-building logic shared by pixel and basin modes.
 * @param {Object} coords - Click coordinates {lat, lon}
 * @param {ee.Geometry} reduceGeom - Geometry for reduceRegion (point or basin polygon)
 * @param {string} locLabel - Location label for chart titles
 * @param {boolean} isBasinMode - Whether we are in basin mode
 */
function _buildCharts(coords, reduceGeom, locLabel, isBasinMode) {
  var point = ee.Geometry.Point([coords.lon, coords.lat]);
  var year = chartYear;
  var leftName = leftProduct;
  var rightName = rightProduct;

  // Determine which products have data for the selected year
  var leftInRange = (year >= ET_PRODUCTS[leftName].startYear &&
                     year <= ET_PRODUCTS[leftName].endYear);
  var rightInRange = (year >= ET_PRODUCTS[rightName].startYear &&
                      year <= ET_PRODUCTS[rightName].endYear);

  if (!leftInRange && !rightInRange) {
    chartPanel.clear();
    chartPanel.add(ui.Label(
      'No data available for either product in ' + year,
      {fontSize: '14px', color: 'gray', textAlign: 'center', stretch: 'horizontal'}
    ));
    return;
  }

  // In basin mode, use coarser scale for reduceRegion to avoid processing
  // millions of pixels (e.g. OpenET at 30 m over large basins). A 500 m
  // scale gives the same spatial mean much faster.
  var BASIN_MIN_SCALE = 500;

  // Build list of active products, yProperties, and colors
  var activeProducts = [];
  var monthYProps = [];
  var monthColors = [];
  if (leftInRange) {
    activeProducts.push(leftName);
    monthYProps.push(leftName);
    monthColors.push('#1f77b4');
  }
  if (rightInRange && rightName !== leftName) {
    activeProducts.push(rightName);
    monthYProps.push(rightName);
    monthColors.push('#ff7f0e');
  }

  // --- Monthly chart ---
  // Extract monthly values only for in-range products (skip annual-only products)
  var monthlyProducts = activeProducts.filter(function (name) {
    return !ET_PRODUCTS[name].annual;
  });
  var monthYPropsFiltered = [];
  var monthColorsFiltered = [];
  monthlyProducts.forEach(function (name) {
    if (name === leftName) { monthYPropsFiltered.push(name); monthColorsFiltered.push('#1f77b4'); }
    else { monthYPropsFiltered.push(name); monthColorsFiltered.push('#ff7f0e'); }
  });
  var monthlyData = {};
  monthlyProducts.forEach(function (name) {
    var monthly = getMonthlyET(name, year);
    var res = isBasinMode
      ? Math.max(ET_PRODUCTS[name].nativeRes, BASIN_MIN_SCALE)
      : ET_PRODUCTS[name].nativeRes;
    monthlyData[name] = monthly.toList(12).map(function (img) {
      img = ee.Image(img);
      var val = img.reduceRegion({
        reducer: ee.Reducer.mean(),
        geometry: reduceGeom,
        scale: res,
        bestEffort: true
      }).values().get(0);
      return ee.Algorithms.If(ee.Algorithms.IsEqual(val, null), 0, val);
    });
  });

  // Build feature collection for monthly chart
  var monthFeatures = ee.List.sequence(0, 11).map(function (i) {
    i = ee.Number(i).toInt();
    var props = {'month': ee.Number(i).add(1)};
    for (var k = 0; k < monthlyProducts.length; k++) {
      props[monthlyProducts[k]] = ee.List(monthlyData[monthlyProducts[k]]).get(i);
    }
    return ee.Feature(null, props);
  });

  var monthFC = ee.FeatureCollection(monthFeatures);

  // Apply unit conversion if imperial
  var monthlyUnit = chartUnits === 'imperial' ? 'inches/month' : 'mm/month';
  var monthlyConv = chartUnits === 'imperial' ? MM_TO_INCH : 1;
  var displayMonthFC = monthlyConv !== 1
    ? monthFC.map(function (f) {
        var converted = {};
        for (var k = 0; k < monthlyProducts.length; k++) {
          converted[monthlyProducts[k]] = ee.Number(f.get(monthlyProducts[k])).multiply(monthlyConv);
        }
        converted['month'] = f.get('month');
        return ee.Feature(null, converted);
      })
    : monthFC;

  var modeTag = isBasinMode ? ' [basin mean]' : '';
  var monthChart = ui.Chart.feature.byFeature({
    features: displayMonthFC,
    xProperty: 'month',
    yProperties: monthYPropsFiltered
  }).setChartType('ColumnChart')
    .setOptions({
      title: 'Monthly ET — ' + year + '  (' + locLabel + ')' + modeTag +
             (monthlyProducts.length < activeProducts.length
               ? '  [annual-only products omitted]' : ''),
      hAxis: {title: 'Month', ticks: [1,2,3,4,5,6,7,8,9,10,11,12]},
      vAxis: {title: 'ET (' + monthlyUnit + ')'},
      colors: monthColorsFiltered,
      bar: {groupWidth: '70%'},
      legend: {position: 'top'}
    });

  // --- Annual chart ---
  // Determine year range based on which products are in range
  var annualStart, annualEnd;
  var annualProducts = [];
  var annualColors = [];

  if (leftInRange && rightInRange) {
    // Both in range — use overlap
    var range = overlapRange(leftName, rightName);
    annualStart = range.start;
    annualEnd = range.end;
    annualProducts = [leftName];
    annualColors = ['#1f77b4'];
    if (rightName !== leftName) {
      annualProducts.push(rightName);
      annualColors.push('#ff7f0e');
    }
  } else if (leftInRange) {
    annualStart = ET_PRODUCTS[leftName].startYear;
    annualEnd = ET_PRODUCTS[leftName].endYear;
    annualProducts = [leftName];
    annualColors = ['#1f77b4'];
  } else {
    annualStart = ET_PRODUCTS[rightName].startYear;
    annualEnd = ET_PRODUCTS[rightName].endYear;
    annualProducts = [rightName];
    annualColors = ['#ff7f0e'];
  }

  // Limit range to at most 30 years centered on the selected year
  if (annualEnd - annualStart > 30) {
    annualStart = Math.max(annualStart, year - 15);
    annualEnd = Math.min(annualEnd, annualStart + 30);
  }

  var years = ee.List.sequence(annualStart, annualEnd);

  var annualFeatures = years.map(function (y) {
    y = ee.Number(y).toInt();
    var props = {'year': y};
    for (var k = 0; k < annualProducts.length; k++) {
      var name = annualProducts[k];
      var res = isBasinMode
        ? Math.max(ET_PRODUCTS[name].nativeRes, BASIN_MIN_SCALE)
        : ET_PRODUCTS[name].nativeRes;
      var annualVal = getMonthlyET(name, y).sum().reduceRegion({
        reducer: ee.Reducer.mean(),
        geometry: reduceGeom,
        scale: res,
        bestEffort: true
      }).values().get(0);
      props[name] = ee.Algorithms.If(ee.Algorithms.IsEqual(annualVal, null), 0, annualVal);
    }
    return ee.Feature(null, props);
  });

  var annualFC = ee.FeatureCollection(annualFeatures);

  // Apply unit conversion if imperial
  var annualUnit = chartUnits === 'imperial' ? 'ft/yr' : 'mm/yr';
  var annualConv = chartUnits === 'imperial' ? MM_TO_FT : 1;
  var displayAnnualFC = annualConv !== 1
    ? annualFC.map(function (f) {
        var converted = {'year': f.get('year')};
        for (var k = 0; k < annualProducts.length; k++) {
          converted[annualProducts[k]] = ee.Number(f.get(annualProducts[k])).multiply(annualConv);
        }
        return ee.Feature(null, converted);
      })
    : annualFC;

  var annualChart = ui.Chart.feature.byFeature({
    features: displayAnnualFC,
    xProperty: 'year',
    yProperties: annualProducts
  }).setChartType('LineChart')
    .setOptions({
      title: 'Annual ET  (' + locLabel + ')' + modeTag,
      hAxis: {title: 'Year', format: '####'},
      vAxis: {title: 'ET (' + annualUnit + ')'},
      colors: annualColors,
      lineWidth: 2,
      pointSize: 4,
      legend: {position: 'top'}
    });

  // Build basin info label
  var basinInfoLabel = ui.Label('', {
    fontSize: '12px', fontWeight: 'bold', color: '#333',
    margin: '2px 0 0 4px', stretch: 'horizontal'
  });

  if (isBasinMode) {
    // In basin mode, locLabel already has the basin name
    basinInfoLabel.setValue(locLabel);
  } else {
    // In pixel mode, identify which basin(s) the clicked point falls in
    var calHit = calibrationBasins.filterBounds(point);
    var subHit = subBasins.filterBounds(point);

    var calName = calHit.size().gt(0)
      ? calHit.first().get('HU_12_NAME')
      : null;
    var subName = subHit.size().gt(0)
      ? subHit.first().get('HU_10_NAME')
      : null;

    ee.Dictionary({cal: calName, sub: subName}).evaluate(function (result) {
      var parts = [];
      if (result && result.sub) parts.push('Sub-basin: ' + result.sub);
      if (result && result.cal) parts.push('Calibration basin: ' + result.cal);
      if (parts.length > 0) {
        basinInfoLabel.setValue(parts.join('  |  '));
      } else {
        basinInfoLabel.setValue('Outside basin boundaries');
      }
    });
  }

  // Render charts with basin info
  chartPanel.clear();
  chartPanel.add(basinInfoLabel);
  var chartsRow = ui.Panel({
    widgets: [monthChart, annualChart],
    layout: ui.Panel.Layout.flow('horizontal'),
    style: {stretch: 'horizontal'}
  });
  chartPanel.add(chartsRow);

  // Add marker to both maps — remove any previous click marker first
  var marker = ee.Geometry.Point([coords.lon, coords.lat]).buffer(2000);
  var markerImg = ee.Image().paint(marker, 1, 2);
  var leftMarker = ui.Map.Layer(markerImg, {palette: ['red']}, 'Click Location');
  var rightMarker = ui.Map.Layer(markerImg, {palette: ['red']}, 'Click Location');

  // Remove previous click markers by name
  function removeLayerByName(map, name) {
    var layers = map.layers();
    for (var i = layers.length() - 1; i >= 0; i--) {
      if (layers.get(i).getName() === name) {
        layers.remove(layers.get(i));
      }
    }
  }
  removeLayerByName(leftMap, 'Click Location');
  removeLayerByName(rightMap, 'Click Location');

  leftMap.layers().add(leftMarker);
  rightMap.layers().add(rightMarker);
}

leftMap.onClick(onMapClick);
rightMap.onClick(onMapClick);

// ───────────────────────────────────────────────────────────────────
// 11. Initial Render
// ───────────────────────────────────────────────────────────────────

refreshMap();
