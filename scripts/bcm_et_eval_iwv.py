"""
Basin-level ET Evaluation Script (Indian Wells Valley)
======================================================

Compares multiple ET products from Google Earth Engine against USGS Reitz ET
and USGS BCM/Flint data for the IWV BCM calibration basins and IWV sub-basins.
Water-year mode only (WY2001-2015). All GEE products are ingested at monthly
scale; the USGS BCM/Flint (270 m) and resampled Reitz ET (200 m) products are read
from local rasters.

Features:
---------
1) Basin-level spatial averages of mean annual AET (ac-ft/yr) as bar charts
2) Maps of ratios of mean annual AET to mean annual PRISM PPT
3) Annual ET time series comparison plots for each basin
4) Difference metrics (MBD, RMSD, MAD) vs USGS Reitz reference
5) Correlation analysis (Pearson, Spearman) for each product
6) Inter-product agreement (CV, range) across basins

ET Products Compared:
---------------------
- USGS BCM/Flint (local 270 m ASC rasters, Data/USGS/BCM_AET/aet_WYs_1896_2024; Flint et al. 2021)
- USGS Reitz ET (local 200 m ADF rasters, Data/USGS/ReitzET_GB_wy_averages_and_POR_ave_mm)
- USGS Reitz Ensemble (projects/nwi-usgs/assets/USGS-Reitz-Ensemble-ET)
- USGS Reitz SSEBop-WB (projects/nwi-usgs/assets/USGS-Reitz-SSEBop-WB)
- MOD16 (MODIS/061/MOD16A2GF)
- PMLv2 (projects/pml_evapotranspiration/PML/OUTPUT/PML_V22a)
- SSEBop VIIRS (projects/usgs-ssebop/viirs_et_v6_monthly)
- SSEBop MODIS (projects/usgs-ssebop/modis_et_v5_monthly)
- OpenET Ensemble/SSEBop/eeMETRIC/DisALEXI/geeSEBAL/PT-JPL (OpenET/.../GRIDMET/MONTHLY/v2_0)
- WLDAS ET (projects/climate-engine-pro/assets/ce-wldas/daily)
- TerraClimate (IDAHO_EPSCOR/TERRACLIMATE)

Example Usage:
--------------
# Default: process both basin sets, GEE + local Reitz ET
python bcm_et_eval_iwv.py

# Only one basin set
python bcm_et_eval_iwv.py --basin-set calibration
python bcm_et_eval_iwv.py --basin-set subbasin

# Test mode (2 basins)
python bcm_et_eval_iwv.py --test

Output Directories:
-------------------
- Data/Outputs/IWV_BCM_ET_EVAL_calibration/
- Data/Outputs/IWV_BCM_ET_EVAL_subbasin/
- Data/Outputs/IWV_BCM_ET_EVAL_whole/

Author: Dr. Sayantan Majumdar (Desert Research Institute)
Contact: sayantan.majumdar@dri.edu
Date: 2026
License: MIT (see LICENSE)

Disclaimer: This software is provided for research purposes only and is not
intended or validated for use in legal, regulatory, or adjudicative
proceedings. See DISCLAIMER.md in the repository root for full terms.
"""

import os
import warnings
import argparse

# Set matplotlib backend to non-interactive 'Agg' for thread-safe plotting
# This must be done BEFORE importing pyplot
import matplotlib
matplotlib.use('Agg')

import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.mask import mask
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
import matplotlib.patheffects as mpe
from shapely.geometry import mapping
import ee
from tqdm import tqdm
import dask
from dask import delayed, compute
from dask.diagnostics import ProgressBar
import multiprocessing

warnings.filterwarnings('ignore')

# Dask configuration
N_WORKERS = min(8, multiprocessing.cpu_count())  # Limit workers to avoid overwhelming GEE API
GEE_MAX_WORKERS = 10  # Concurrent GEE requests per basin (be conservative to avoid rate limits)

# Configure Dask scheduler
dask.config.set(scheduler='threads')  # Use threaded scheduler for I/O-bound tasks

# ============================================================================
# Configuration
# ============================================================================

# Paths
BASE_DIR = 'Data/'  # Base directory for all USGS data; adjust as needed
ET_DIR = os.path.join(BASE_DIR, 'USGS/ReitzET_GB_wy_averages_and_POR_ave_mm')
BCM_AET_DIR = os.path.join(BASE_DIR, 'USGS/BCM_AET/aet_WYs_1896_2024')
BCM_AET_CRS = 'EPSG:3310'  # California Teale Albers
BASINS_CALIBRATION_PATH = os.path.join(BASE_DIR, 'USGS/IWV_calibrationBasin/IWV_Calibration.shp')
BASINS_SUBBASIN_PATH = os.path.join(BASE_DIR, 'USGS/IWV_SubBasin/IWV_subbasin.shp')

# Basin set configurations: set key -> (shapefile path, name column in shapefile)
BASIN_SETS = {
    'calibration': (BASINS_CALIBRATION_PATH, 'HU_12_NAME'),
    'subbasin': (BASINS_SUBBASIN_PATH, 'HU_10_NAME'),
}

# Output directory template - {set} is replaced with the basin set key
OUTPUT_DIR_TEMPLATE = os.path.join(BASE_DIR, 'Outputs/IWV_BCM_ET_EVAL_{set}/')
OUTPUT_DIR_WHOLE = os.path.join(BASE_DIR, 'Outputs/IWV_BCM_ET_EVAL_whole/')

# Water years available (from local Reitz ET data)
WATER_YEARS = list(range(2001, 2016))

# Conversion factors
MM_TO_M = 0.001
SQ_M_TO_ACRES = 0.000247105

# Figure settings for journal quality
plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['Arial', 'Helvetica'],
    'font.size': 10,
    'axes.labelsize': 11,
    'axes.titlesize': 12,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
    'legend.fontsize': 9,
    'figure.dpi': 600,
    'savefig.dpi': 600,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.1,
})

# Color palette for ET products
ET_COLORS = {
    'USGS BCM/Flint': '#0b5394',
    'USGS BCM/Reitz': '#1f77b4',
    'USGS Reitz Ensemble': '#1a5276',  # GEE asset projects/nwi-usgs/assets/USGS-Reitz-Ensemble-ET
    'USGS SSEBop-WB/Reitz': '#5dade2',  # GEE asset projects/nwi-usgs/assets/USGS-Reitz-SSEBop-WB
    'MOD16': '#ff7f0e',
    'PMLv2': '#2ca02c',
    'SSEBop VIIRS': '#d62728',
    'SSEBop MODIS': '#9467bd',
    'OpenET Ensemble': '#8c564b',
    'OpenET SSEBop': '#17becf',
    'OpenET eeMETRIC': '#bcbd22',
    'OpenET DisALEXI': '#ff9896',
    'OpenET geeSEBAL': '#98df8a',
    'OpenET PT-JPL': '#c5b0d5',
    'WLDAS': '#e377c2',
    'TerraClimate': '#7f7f7f',
}

# ============================================================================
# Google Earth Engine Initialization
# ============================================================================

def initialize_gee():
    """Initialize Google Earth Engine."""
    try:
        ee.Initialize()
        print("GEE initialized successfully")
        return True
    except Exception as e:
        print(f"GEE default initialization failed: {e}")
        try:
            ee.Authenticate()
            ee.Initialize()
            print("GEE initialized after authentication")
            return True
        except Exception as e2:
            print(f"GEE authentication failed: {e2}")
            print("Please authenticate using: earthengine authenticate")
            return False


# ============================================================================
# Data Loading Functions
# ============================================================================

def load_basins(basins_path, name_col='BasinName'):
    """Load basins from a vector file.

    Parameters
    ----------
    basins_path : str
        Path to the basin vector file (GeoJSON, shapefile, etc.).
    name_col : str
        Name of the column in the source file that identifies each basin.
        This column will be renamed to ``BasinName`` so the rest of the
        pipeline can rely on a consistent column name.
    """
    gdf = gpd.read_file(basins_path)
    # Ensure CRS is WGS84 for GEE compatibility
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')
    # Standardize the basin-name column for downstream code
    if name_col != 'BasinName':
        if name_col not in gdf.columns:
            raise ValueError(
                f"Column '{name_col}' not found in {basins_path}. "
                f"Available columns: {list(gdf.columns)}"
            )
        gdf = gdf.rename(columns={name_col: 'BasinName'})
    # Dissolve duplicate basin names into single geometries to avoid
    # picking up degenerate slivers that yield zero-area calculations
    if gdf['BasinName'].duplicated().any():
        gdf = gdf.dissolve(by='BasinName', as_index=False)
    return gdf


def load_reitz_et_raster(year, et_dir=ET_DIR):
    """Load USGS Reitz ET raster for a given water year."""
    raster_dir = os.path.join(et_dir, f'et_wy{year}_mm')
    raster_path = os.path.join(raster_dir, 'w001001.adf')
    
    if os.path.exists(raster_path):
        return rasterio.open(raster_path)
    return None


def get_water_year_dates(wy):
    """Get start and end dates for a water year (Oct 1 - Sep 30).

    Note: GEE filterDate is exclusive on end_date, so we use Oct 1 of the
    water year to include all days through Sep 30.
    """
    start_date = f'{wy-1}-10-01'
    end_date = f'{wy}-10-01'  # Exclusive, so this includes Sep 30
    return start_date, end_date


# ============================================================================
# GEE Data Extraction Functions
# ============================================================================

def get_basin_geometry_ee(basin_gdf):
    """Convert basin geometry to Earth Engine geometry."""
    geom = basin_gdf.geometry.values[0]
    return ee.Geometry(mapping(geom))


def extract_et_from_gee(basin_geom, start_date, end_date, product='mod16', scale=200):
    """
    Extract ET values from various GEE products for a basin.
    Returns water year total ET in mm/year (to match Reitz ET annual average).
    
    Parameters:
    -----------
    basin_geom : ee.Geometry
        Earth Engine geometry for the basin
    start_date : str
        Start date for filtering
    end_date : str
        End date for filtering
    product : str
        ET product name
    scale : int
        Resolution in meters for GEE reduceRegion (200 for Reitz ET, 1000 for Reitz Original)
    """
    
    if product == 'mod16':
        # MOD16 - 8-day composite, ET in kg/m²/8days = mm/8days
        collection = ee.ImageCollection('MODIS/061/MOD16A2GF') \
            .filterDate(start_date, end_date) \
            .select('ET')
        # Scale factor is 0.1, sum all 8-day composites for annual total
        et_result = collection.sum().multiply(0.1)
        
    elif product == 'pmlv2':
        # PMLv2 - 8-day composite, ET band (total ET)
        # Units: stored as value*100, so multiply by 0.01; 8-day composite so multiply by 8
        # Net scale factor: 0.08
        collection = ee.ImageCollection('projects/pml_evapotranspiration/PML/OUTPUT/PML_V22a') \
            .filterDate(start_date, end_date) \
            .select('ET')
        # Sum all composites and apply scale factor (0.01 * 8 = 0.08)
        et_result = collection.sum().multiply(0.08)
    
    elif product == 'reitz_ensemble':
        # USGS Reitz Ensemble ET (GEE asset) - monthly, mm/day
        # Multiply by days in each month for mm/month, then sum for annual total
        collection = ee.ImageCollection('projects/nwi-usgs/assets/USGS-Reitz-Ensemble-ET') \
            .filterDate(start_date, end_date) \
            .select('b1')
        if collection.size().getInfo() == 0:
            return None
        # Each image is monthly average mm/day; multiply by days in month
        # Then sum all months for annual total
        def scale_by_days(img):
            date = ee.Date(img.get('system:time_start'))
            days_in_month = date.advance(1, 'month').difference(date, 'day')
            return img.multiply(days_in_month)
        et_mm = collection.map(scale_by_days).sum()
        # Source raster masks truly-dry pixels (e.g. playas); unmask(0) treats
        # them as zero ET so the basin mean matches the local 200 m rasters.
        et_result = et_mm.unmask(0)

    elif product == 'reitz_ssebop_wb':
        # USGS Reitz SSEBop Water Balance ET (GEE asset) - monthly, meters/month
        # Convert to mm/month (multiply by 1000), then sum for annual total
        collection = ee.ImageCollection('projects/nwi-usgs/assets/USGS-Reitz-SSEBop-WB') \
            .filterDate(start_date, end_date) \
            .select('b1')
        if collection.size().getInfo() == 0:
            return None
        # Each image is m/month; multiply by 1000 for mm/month, sum for annual total
        et_mm = collection.sum().multiply(1000)
        # Source raster masks truly-dry pixels (e.g. playas); unmask(0) treats
        # them as zero ET so the basin mean matches the local 200 m rasters.
        et_result = et_mm.unmask(0)
        
    elif product == 'ssebop_viirs':
        # SSEBop VIIRS - monthly ET in mm (available from ~2012)
        collection = ee.ImageCollection('projects/usgs-ssebop/viirs_et_v6_monthly') \
            .filterDate(start_date, end_date) \
            .select('et')
        if collection.size().getInfo() == 0:
            return None
        # Sum all months for annual total
        et_result = collection.sum()
        
    elif product == 'ssebop_modis':
        # SSEBop MODIS - monthly ET in mm
        collection = ee.ImageCollection('projects/usgs-ssebop/modis_et_v5_monthly') \
            .filterDate(start_date, end_date) \
            .select('et')
        # Sum all months for annual total
        et_result = collection.sum()
        
    elif product == 'openet':
        # OpenET Ensemble - monthly ET in mm
        collection = ee.ImageCollection('OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0') \
            .filterDate(start_date, end_date) \
            .select('et_ensemble_mad')
        if collection.size().getInfo() == 0:
            return None
        # Sum all months for annual total
        et_result = collection.sum()
        
    elif product == 'openet_ssebop':
        # OpenET SSEBop - monthly ET in mm
        collection = ee.ImageCollection('OpenET/SSEBOP/CONUS/GRIDMET/MONTHLY/v2_0') \
            .filterDate(start_date, end_date) \
            .select('et')
        if collection.size().getInfo() == 0:
            return None
        # Sum all months for annual total
        et_result = collection.sum()
        
    elif product == 'openet_eemetric':
        # OpenET eeMETRIC - monthly ET in mm
        collection = ee.ImageCollection('OpenET/EEMETRIC/CONUS/GRIDMET/MONTHLY/v2_0') \
            .filterDate(start_date, end_date) \
            .select('et')
        if collection.size().getInfo() == 0:
            return None
        # Sum all months for annual total
        et_result = collection.sum()
        
    elif product == 'openet_disalexi':
        # OpenET DisALEXI - monthly ET in mm
        collection = ee.ImageCollection('OpenET/DISALEXI/CONUS/GRIDMET/MONTHLY/v2_0') \
            .filterDate(start_date, end_date) \
            .select('et')
        if collection.size().getInfo() == 0:
            return None
        # Sum all months for annual total
        et_result = collection.sum()
        
    elif product == 'openet_geesebal':
        # OpenET geeSEBAL - monthly ET in mm
        collection = ee.ImageCollection('OpenET/GEESEBAL/CONUS/GRIDMET/MONTHLY/v2_0') \
            .filterDate(start_date, end_date) \
            .select('et')
        if collection.size().getInfo() == 0:
            return None
        # Sum all months for annual total
        et_result = collection.sum()
        
    elif product == 'openet_ptjpl':
        # OpenET PT-JPL - monthly ET in mm
        collection = ee.ImageCollection('OpenET/PTJPL/CONUS/GRIDMET/MONTHLY/v2_0') \
            .filterDate(start_date, end_date) \
            .select('et')
        if collection.size().getInfo() == 0:
            return None
        # Sum all months for annual total
        et_result = collection.sum()
        
    elif product == 'wldas':
        # WLDAS - daily Evap_tavg in kg/m²/s
        # Convert to mm/day (1 kg/m²/s = 86400 mm/day), sum for annual total
        collection = ee.ImageCollection('projects/climate-engine-pro/assets/ce-wldas/daily') \
            .filterDate(start_date, end_date) \
            .select('Evap_tavg')
        # Sum all days and convert to mm/year
        et_result = collection.sum().multiply(86400)
        
    elif product == 'terraclimate':
        # TerraClimate - monthly AET in mm (scale factor 0.1)
        collection = ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE') \
            .filterDate(start_date, end_date) \
            .select('aet')
        # Sum all months for annual total, apply scale factor
        et_result = collection.sum().multiply(0.1)

    elif product == 'prism_ppt':
        # PRISM precipitation - monthly in mm
        collection = ee.ImageCollection('projects/sat-io/open-datasets/OREGONSTATE/PRISM_800_MONTHLY') \
            .filterDate(start_date, end_date) \
            .select('ppt')
        # Sum all months for annual total
        et_result = collection.sum()
        
    else:
        raise ValueError(f"Unknown product: {product}")
    
    # Calculate mean over basin
    mean_val = et_result.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=basin_geom,
        scale=scale,  # 200m for Reitz ET, 1000m for Reitz Original
        maxPixels=1e9
    )
    
    return mean_val


def _extract_single_product_year(basin_geom, year, product, scale=200):
    """
    Extract a single ET product for a single water year.
    Helper function for parallel execution.

    Parameters:
    -----------
    basin_geom : ee.Geometry
        Earth Engine geometry for the basin
    year : int
        Water year
    product : str
        ET product name
    scale : int
        Resolution in meters for GEE reduceRegion
    """
    start_date, end_date = get_water_year_dates(year)
    try:
        val = extract_et_from_gee(basin_geom, start_date, end_date, product, scale=scale)
        if val is None:
            return (year, product, np.nan)
        val_dict = val.getInfo()
        if val_dict:
            v = list(val_dict.values())[0]
            # reduceRegion returns None if the basin has no pixels in the
            if v is None:
                return (year, product, np.nan)
            return (year, product, float(v))
        return (year, product, np.nan)
    except Exception:
        return (year, product, np.nan)


def extract_all_et_products(basin_gdf, years, use_parallel=True, scale=200):
    """
    Extract all ET products for a basin across all water years.
    Uses Dask delayed for concurrent GEE API calls.

    Parameters:
    -----------
    basin_gdf : GeoDataFrame
        Single-row GeoDataFrame for the basin
    years : list
        List of water years to process
    use_parallel : bool
        Whether to use parallel processing
    scale : int
        Resolution in meters for GEE reduceRegion
    """
    basin_geom = get_basin_geometry_ee(basin_gdf)
    basin_name = basin_gdf['BasinName'].values[0]

    products = ['mod16', 'pmlv2', 'ssebop_viirs', 'ssebop_modis',
                'openet', 'openet_ssebop', 'openet_eemetric', 'openet_disalexi',
                'openet_geesebal', 'openet_ptjpl', 'wldas', 'terraclimate',
                'reitz_ensemble', 'reitz_ssebop_wb']
    all_products = products + ['prism_ppt']

    results = {p: {y: np.nan for y in years} for p in all_products}
    results['year'] = years

    if use_parallel:
        # Create delayed tasks for all product-year combinations
        delayed_tasks = [
            delayed(_extract_single_product_year)(basin_geom, y, product, scale)
            for y in years
            for product in all_products
        ]

        # Compute all tasks in parallel using Dask
        computed_results = compute(*delayed_tasks, scheduler='threads', num_workers=GEE_MAX_WORKERS)

        # Process results
        for y, product, value in computed_results:
            results[product][y] = value
    else:
        # Sequential fallback
        for y in tqdm(years, desc=f"  WY data for {basin_name[:20]}", leave=False):
            for product in all_products:
                _, _, value = _extract_single_product_year(basin_geom, y, product, scale)
                results[product][y] = value
    
    # Convert to DataFrame with proper ordering
    df_data = {'year': years}
    for product in all_products:
        df_data[product] = [results[product][y] for y in years]
    
    return pd.DataFrame(df_data)


# ============================================================================
# Local Raster Processing Functions
# ============================================================================

def _extract_mean_from_raster(raster_path, geom, scale_factor=1.0):
    """
    Extract mean value from a raster file within a geometry mask.
    
    This is a core utility function used by all Reitz ET extraction methods.
    
    Parameters:
    -----------
    raster_path : str
        Path to the raster file (TIF or ADF)
    geom : list
        Basin geometry in GeoJSON format (for masking)
    scale_factor : float
        Factor to multiply values by (e.g., 1000 for m to mm conversion)
    
    Returns:
    --------
    float : Mean value within the geometry, or np.nan if extraction fails
    """
    try:
        if not os.path.exists(raster_path):
            return np.nan
            
        with rasterio.open(raster_path) as src:
            out_image, out_transform = mask(src, geom, crop=True, nodata=np.nan)
            data = out_image[0]
            
            # Replace nodata values
            nodata = src.nodata
            if nodata is not None:
                data = np.where(np.isclose(data, nodata, rtol=1e-5), np.nan, data)
            
            return np.nanmean(data) * scale_factor
    except Exception:
        return np.nan


def _extract_reitz_et_single_year(geom, year, raster_dir, raster_pattern='et_wy{year}_mm/w001001.adf'):
    """
    Extract Reitz ET for a single year from a raster file.
    Unified helper function for Dask parallel execution.
    
    Parameters:
    -----------
    geom : list
        Basin geometry in GeoJSON format (for masking)
    year : int
        Year (water year or calendar year)
    raster_dir : str
        Base directory containing the raster files
    raster_pattern : str
        Pattern for raster path with {year} placeholder
        Default: 'et_wy{year}_mm/w001001.adf' for ADF rasters
        For TIFs: 'USGS_Reitz_2023_CY{year}_mm.tif'
    """
    raster_path = os.path.join(raster_dir, raster_pattern.format(year=year))
    return _extract_mean_from_raster(raster_path, geom)


def extract_bcm_aet_for_basin(basin_gdf, years, raster_dir=BCM_AET_DIR,
                              use_parallel=True):
    """
    Extract USGS BCM/Flint values for a basin from local ASC rasters.

    The USGS BCM/Flint rasters use CA Teale Albers (EPSG:3310) and are at 270 m
    resolution. Data source: Flint et al. (2021a, 2021b).

    Parameters
    ----------
    basin_gdf : GeoDataFrame
        Single-row GeoDataFrame for the basin
    years : list
        List of water years
    raster_dir : str
        Directory containing aet_wy{year}.asc files
    use_parallel : bool
        Whether to use parallel processing

    Returns
    -------
    list : AET values in mm for each year
    """
    basin_proj = basin_gdf.to_crs(BCM_AET_CRS)
    geom = [mapping(basin_proj.geometry.values[0])]

    if use_parallel:
        delayed_results = [
            delayed(_extract_reitz_et_single_year)(geom, y, raster_dir, 'aet_wy{year}.asc')
            for y in years
        ]
        results = compute(*delayed_results, scheduler='threads', num_workers=N_WORKERS)
        return list(results)
    else:
        return [_extract_reitz_et_single_year(geom, y, raster_dir, 'aet_wy{year}.asc')
                for y in years]


def extract_reitz_et_for_basin(basin_gdf, years, raster_dir=ET_DIR,
                               raster_pattern='et_wy{year}_mm/w001001.adf',
                               use_parallel=True):
    """
    Extract USGS Reitz ET values for a basin from local rasters using Dask.
    
    This is the unified extraction function that works with both ADF rasters
    and generated TIF files by specifying the appropriate directory and pattern.
    
    Parameters:
    -----------
    basin_gdf : GeoDataFrame
        Single-row GeoDataFrame for the basin
    years : list
        List of years (water years or calendar years)
    raster_dir : str
        Base directory containing the raster files
    raster_pattern : str
        Pattern for raster path with {year} placeholder
        Default: 'et_wy{year}_mm/w001001.adf' for original ADF rasters
        For TIFs: 'USGS_Reitz_2023_CY{year}_mm.tif' or similar
    use_parallel : bool
        Whether to use parallel processing
    
    Returns:
    --------
    list : ET values in mm for each year
    
    Examples:
    ---------
    # Extract from original ADF rasters (water year)
    et_values = extract_reitz_et_for_basin(basin_gdf, [2001, 2002, 2003])
    
    # Extract from generated 2023 CY TIF files
    et_values = extract_reitz_et_for_basin(
        basin_gdf, [2000, 2001, 2002],
        raster_dir='/path/to/USGS_Reitz_2023_CY_TIFs',
        raster_pattern='USGS_Reitz_2023_CY{year}_mm.tif'
    )
    """
    # Reproject basin to match raster CRS (EPSG:5070)
    basin_proj = basin_gdf.to_crs('EPSG:5070')
    geom = [mapping(basin_proj.geometry.values[0])]
    
    if use_parallel:
        delayed_results = [
            delayed(_extract_reitz_et_single_year)(geom, y, raster_dir, raster_pattern) 
            for y in years
        ]
        results = compute(*delayed_results, scheduler='threads', num_workers=N_WORKERS)
        return list(results)
    else:
        return [_extract_reitz_et_single_year(geom, y, raster_dir, raster_pattern) for y in years]



def calculate_basin_area_acres(basin_gdf):
    """Calculate basin area in acres."""
    # Reproject to equal area projection for accurate area calculation
    basin_proj = basin_gdf.to_crs('EPSG:5070')
    area_sq_m = basin_proj.geometry.values[0].area
    area_acres = area_sq_m * SQ_M_TO_ACRES
    return area_acres


# ============================================================================
# Conversion Functions
# ============================================================================

def mm_to_acre_ft(et_mm, area_acres):
    """Convert ET from mm to acre-feet for a given area."""
    # mm to feet: mm * 0.001 / 0.3048
    # acre-feet = feet * acres
    et_ft = et_mm * MM_TO_M / 0.3048
    et_acre_ft = et_ft * area_acres
    return et_acre_ft


# 1 acre-foot = 1233.48183754752 m^3
ACRE_FT_TO_M3 = 1233.48183754752


# ============================================================================
# Plotting Functions
# ============================================================================

def plot_basin_et_barchart(basin_name, et_data, area_acres, output_path, year_label='WY2001-2015'):
    """
    Create bar chart of mean annual AET for different products.
    
    Parameters:
    -----------
    basin_name : str
        Name of the basin
    et_data : dict
        Dictionary with ET product names as keys and mean annual ET (mm) as values
    area_acres : float
        Basin area in acres
    output_path : str
        Path to save the figure
    year_label : str
        Label for the year range (e.g., 'WY2001-2015', 'CY2000-2018')
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    products = list(et_data.keys())
    et_mm = list(et_data.values())
    
    # Convert to acre-ft/yr
    et_acre_ft = [mm_to_acre_ft(et, area_acres) for et in et_mm]
    
    # Create colors list
    colors = [ET_COLORS.get(p, '#333333') for p in products]
    
    # Create bar chart
    bars = ax.bar(range(len(products)), et_acre_ft, color=colors, edgecolor='black', linewidth=0.5)
    
    # Adaptive format: use decimals when values are small
    max_val = max((abs(v) for v in et_acre_ft if not np.isnan(v)), default=0)
    if max_val < 10:
        bar_fmt = '{:,.2f}'
        axis_fmt = lambda x, p: f'{x:,.2f}'
    elif max_val < 100:
        bar_fmt = '{:,.1f}'
        axis_fmt = lambda x, p: f'{x:,.1f}'
    else:
        bar_fmt = '{:,.0f}'
        axis_fmt = lambda x, p: format(int(x), ',')

    # Add value labels on bars
    for bar, val in zip(bars, et_acre_ft):
        height = bar.get_height()
        ax.annotate(bar_fmt.format(val),
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8)

    ax.set_xticks(range(len(products)))
    ax.set_xticklabels(products, rotation=45, ha='right')
    ax.set_ylabel('Mean Annual AET (acre-ft/yr)')
    ax.set_title(f'{basin_name}\nMean Annual AET by Product ({year_label})')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(axis_fmt))

    # Secondary y-axis in m^3/yr (1 acre-ft = 1233.48 m^3)
    ax_m3 = ax.twinx()
    lo, hi = ax.get_ylim()
    ax_m3.set_ylim(lo * ACRE_FT_TO_M3, hi * ACRE_FT_TO_M3)
    ax_m3.set_ylabel('Mean Annual AET (m³/yr)')
    ax_m3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{x:,.2e}'))

    # Add basin info
    textstr = f'Basin Area: {area_acres:,.0f} acres'
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    ax.text(0.95, 0.95, textstr, transform=ax.transAxes, fontsize=9,
            verticalalignment='top', horizontalalignment='right', bbox=props)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=600, bbox_inches='tight')
    plt.close()
    
    return et_acre_ft


def plot_et_ppt_ratio_map(basins_gdf, ratio_data, product_name, output_path, year_label='WY2001-2015'):
    """
    Create map of AET/PPT ratio for all basins.
    
    Parameters:
    -----------
    basins_gdf : GeoDataFrame
        GeoDataFrame with basin geometries
    ratio_data : dict
        Dictionary with basin names as keys and AET/PPT ratios as values
    product_name : str
        Name of the ET product
    output_path : str
        Path to save the figure
    year_label : str
        Label for the year range (e.g., 'WY2001-2015', 'CY2000-2018')
    """
    # Size the figure to roughly match the basins' aspect ratio
    minx, miny, maxx, maxy = basins_gdf.total_bounds
    aspect = max((maxx - minx) / max(maxy - miny, 1e-9), 0.25)
    fig_w = 8
    fig_h = fig_w / aspect + 1.0  # extra room for title/colorbar
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    # Add ratio values to GeoDataFrame
    basins_gdf = basins_gdf.copy()
    basins_gdf['et_ppt_ratio'] = basins_gdf['BasinName'].map(ratio_data)
    
    # Create colormap centered at 1.0 (AET = PPT)
    cmap = plt.cm.RdYlBu_r
    norm = mcolors.TwoSlopeNorm(vmin=0, vcenter=1.0, vmax=1.5)
    
    # Plot basins
    basins_gdf.plot(column='et_ppt_ratio', ax=ax, cmap=cmap, norm=norm,
                    edgecolor='black', linewidth=0.5, legend=False,
                    missing_kwds={'color': 'lightgray', 'label': 'No Data'})
    
    # Add colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, shrink=0.7, label='AET/PPT Ratio')
    cbar.ax.axhline(y=1.0, color='black', linewidth=2, linestyle='--')
    
    ax.set_title(f'Mean Annual AET/PPT Ratio\n{product_name} ({year_label})')

    # Disable lat/long grids and labels
    ax.set_axis_off()

    # Fit tightly around the basins with a small margin
    minx, miny, maxx, maxy = basins_gdf.total_bounds
    dx = (maxx - minx) * 0.05
    dy = (maxy - miny) * 0.05
    ax.set_xlim(minx - dx, maxx + dx)
    ax.set_ylim(miny - dy, maxy + dy)
    ax.set_aspect('equal')

    plt.tight_layout()
    plt.savefig(output_path, dpi=600, bbox_inches='tight')
    plt.close()


def plot_combined_basin_ratio_map(cal_gdf, sub_gdf, cal_ratio, sub_ratio,
                                  product_name, output_path,
                                  year_label='WY2001-2015'):
    """
    Create a single AET/PPT ratio map showing both calibration and sub-basins.

    Calibration basins are outlined in red, sub-basins in black. Both are
    filled by their AET/PPT ratio using the same colormap.

    Parameters
    ----------
    cal_gdf : GeoDataFrame
        Calibration basin geometries
    sub_gdf : GeoDataFrame
        Sub-basin geometries
    cal_ratio : dict
        {basin_name: ratio} for calibration basins
    sub_ratio : dict
        {basin_name: ratio} for sub-basins
    product_name : str
        ET product display name
    output_path : str
        Path to save the figure
    year_label : str
        e.g. 'WY2001-2015'
    """
    # Merge both GDFs with ratio values and a basin-type tag
    cal = cal_gdf.copy()
    cal['et_ppt_ratio'] = cal['BasinName'].map(cal_ratio)
    cal['basin_type'] = 'calibration'

    sub = sub_gdf.copy()
    sub['et_ppt_ratio'] = sub['BasinName'].map(sub_ratio)
    sub['basin_type'] = 'subbasin'

    # Common colormap
    cmap = plt.cm.RdYlBu_r
    norm = mcolors.TwoSlopeNorm(vmin=0, vcenter=1.0, vmax=1.5)

    # Figure sizing from combined bounds
    combined = pd.concat([cal, sub], ignore_index=True)
    minx, miny, maxx, maxy = combined.total_bounds
    aspect = max((maxx - minx) / max(maxy - miny, 1e-9), 0.25)
    fig_w = 10
    fig_h = fig_w / aspect + 1.5
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    # Plot sub-basins first (dark gray outlines)
    sub.plot(column='et_ppt_ratio', ax=ax, cmap=cmap, norm=norm,
             edgecolor='#333333', linewidth=1.5, legend=False,
             missing_kwds={'color': 'lightgray'})

    # Overlay calibration basins (magenta outlines, thicker)
    cal.plot(column='et_ppt_ratio', ax=ax, cmap=cmap, norm=norm,
             edgecolor='#8B008B', linewidth=2.0, legend=False,
             missing_kwds={'color': 'lightgray'})

    # Add basin name labels
    for _, row in combined.iterrows():
        if pd.notna(row.get('et_ppt_ratio')):
            centroid = row.geometry.centroid
            ax.annotate(row['BasinName'], xy=(centroid.x, centroid.y),
                        fontsize=5, ha='center', va='center',
                        color='white', fontweight='bold',
                        path_effects=[
                            mpe.withStroke(linewidth=1.5, foreground='black')])

    # Colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, shrink=0.7, label='AET/PPT Ratio')
    cbar.ax.axhline(y=1.0, color='black', linewidth=2, linestyle='--')

    ax.set_title(f'Mean Annual AET/PPT Ratio — {product_name} ({year_label})')
    ax.set_axis_off()

    # Legend with colored lines
    from matplotlib.lines import Line2D
    legend_handles = [
        Line2D([0], [0], color='#333333', linewidth=1.5, label='Sub-basins'),
        Line2D([0], [0], color='#8B008B', linewidth=2.0, label='BCM calibration basins'),
    ]
    ax.legend(handles=legend_handles, loc='lower left', fontsize=8, frameon=True,
              handlelength=1.0, handletextpad=0.3, borderpad=0.2, borderaxespad=0.2,
              labelspacing=0.2)

    dx = (maxx - minx) * 0.05
    dy = (maxy - miny) * 0.05
    ax.set_xlim(minx - dx, maxx + dx)
    ax.set_ylim(miny - dy, maxy + dy)
    ax.set_aspect('equal')

    plt.tight_layout()
    plt.savefig(output_path, dpi=600, bbox_inches='tight')
    plt.close()


def plot_all_combined_basin_ratio_maps(cal_gdf, sub_gdf, cal_all_ratio,
                                       sub_all_ratio, output_dir,
                                       year_label='WY2001-2015'):
    """
    Create 4x4 multi-panel figure with combined cal + sub-basin ratio maps
    for all products, plus individual per-product maps.

    Parameters
    ----------
    cal_gdf, sub_gdf : GeoDataFrame
        Basin geometries
    cal_all_ratio, sub_all_ratio : dict
        {product: {basin: ratio}} for each basin set
    output_dir : str
        Output directory
    year_label : str
        e.g. 'WY2001-2015'
    """
    os.makedirs(output_dir, exist_ok=True)

    # Union of all products present in either set
    all_products = list(dict.fromkeys(
        list(cal_all_ratio.keys()) + list(sub_all_ratio.keys())))

    # Individual maps
    for product in all_products:
        cal_ratio = cal_all_ratio.get(product, {})
        sub_ratio = sub_all_ratio.get(product, {})
        safe_name = product.replace(' ', '_').replace('/', '_').lower()
        plot_combined_basin_ratio_map(
            cal_gdf, sub_gdf, cal_ratio, sub_ratio, product,
            os.path.join(output_dir, f'combined_et_ppt_ratio_{safe_name}.png'),
            year_label=year_label)

    # Multi-panel figure
    n_products = len(all_products)
    n_cols = 4
    n_rows = (n_products + n_cols - 1) // n_cols

    cal_tmp = cal_gdf.copy()
    sub_tmp = sub_gdf.copy()
    combined_bounds = pd.concat([cal_tmp, sub_tmp], ignore_index=True).total_bounds
    minx, miny, maxx, maxy = combined_bounds
    aspect = max((maxx - minx) / max(maxy - miny, 1e-9), 0.25)
    panel_w = 4
    panel_h = panel_w / aspect
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(panel_w * n_cols, panel_h * n_rows))
    axes = axes.flatten() if n_products > 1 else [axes]

    cmap = plt.cm.RdYlBu_r
    norm = mcolors.TwoSlopeNorm(vmin=0, vcenter=1.0, vmax=1.5)

    for idx, product in enumerate(all_products):
        ax = axes[idx]
        cal_ratio = cal_all_ratio.get(product, {})
        sub_ratio = sub_all_ratio.get(product, {})

        sub_plot = sub_gdf.copy()
        sub_plot['et_ppt_ratio'] = sub_plot['BasinName'].map(sub_ratio)
        sub_plot.plot(column='et_ppt_ratio', ax=ax, cmap=cmap, norm=norm,
                      edgecolor='#333333', linewidth=0.8, legend=False,
                      missing_kwds={'color': 'lightgray'})

        cal_plot = cal_gdf.copy()
        cal_plot['et_ppt_ratio'] = cal_plot['BasinName'].map(cal_ratio)
        cal_plot.plot(column='et_ppt_ratio', ax=ax, cmap=cmap, norm=norm,
                      edgecolor='#8B008B', linewidth=1.0, legend=False,
                      missing_kwds={'color': 'lightgray'})

        ax.set_title(product, fontsize=12, fontweight='bold')
        ax.set_axis_off()
        dx = (maxx - minx) * 0.05
        dy = (maxy - miny) * 0.05
        ax.set_xlim(minx - dx, maxx + dx)
        ax.set_ylim(miny - dy, maxy + dy)
        ax.set_aspect('equal')

    # Hide unused axes
    for idx in range(n_products, len(axes)):
        axes[idx].set_visible(False)

    # Shared colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar_ax = fig.add_axes([0.92, 0.15, 0.015, 0.7])
    cbar = fig.colorbar(sm, cax=cbar_ax, label='AET/PPT Ratio')
    cbar.ax.axhline(y=1.0, color='black', linewidth=2, linestyle='--')
    cbar.ax.tick_params(labelsize=11)
    cbar_ax.yaxis.label.set_size(12)

    fig.suptitle(f'Mean Annual AET/PPT Ratio by ET Product ({year_label})',
                 fontsize=18, fontweight='bold', y=0.98)

    # Shared legend with colored lines
    from matplotlib.lines import Line2D
    legend_handles = [
        Line2D([0], [0], color='#333333', linewidth=2.0, label='Sub-basins'),
        Line2D([0], [0], color='#8B008B', linewidth=2.5, label='BCM calibration basins'),
    ]
    fig.legend(handles=legend_handles, loc='upper center', ncol=2,
               fontsize=12, frameon=True, bbox_to_anchor=(0.45, 0.96),
               handlelength=1.0, handletextpad=0.3, borderpad=0.2, columnspacing=0.5)

    plt.savefig(os.path.join(output_dir, 'all_products_combined_et_ppt_ratio_maps.png'),
                dpi=600, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'all_products_combined_et_ppt_ratio_maps.pdf'),
                dpi=600, bbox_inches='tight')
    plt.close()


def plot_multi_product_comparison(basin_name, et_timeseries, output_path, year_label='WY2001-2015'):
    """
    Create time series comparison plot for all ET products.
    Shows both mm (left axis) and inches (right axis).
    
    Parameters:
    -----------
    basin_name : str
        Name of the basin
    et_timeseries : DataFrame
        DataFrame with years as index and ET products as columns
    output_path : str
        Path to save the figure
    year_label : str
        Label for the year range (e.g., 'WY2001-2015', 'CY2000-2018')
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for col in et_timeseries.columns:
        if col != 'year':
            color = ET_COLORS.get(col, '#333333')
            ax.plot(et_timeseries['year'], et_timeseries[col], 
                   marker='o', markersize=4, label=col, color=color, linewidth=1.5)
    
    # Determine year type from year_label prefix
    year_type = "Calendar Year" if year_label.startswith('CY') else "Water Year"
    
    ax.set_xlabel(year_type)
    ax.set_ylabel('Annual ET and PPT (mm)')
    ax.set_title(f'{basin_name}\nAnnual ET and PPT Time Series ({year_label})')
    ax.legend(loc='upper left', bbox_to_anchor=(1.12, 1), frameon=True)
    ax.grid(True, alpha=0.3)
    
    # Add secondary y-axis for inches
    ax2 = ax.twinx()
    # Convert mm to inches (1 inch = 25.4 mm)
    mm_min, mm_max = ax.get_ylim()
    ax2.set_ylim(mm_min / 25.4, mm_max / 25.4)
    ax2.set_ylabel('Annual ET and PPT (inches)')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=600, bbox_inches='tight')
    plt.close()


def plot_all_products_ratio_maps(basins_gdf, all_ratio_data, output_dir, year_label='WY2001-2015'):
    """
    Create a multi-panel figure showing AET/PPT ratio maps for all products.
    
    Parameters:
    -----------
    basins_gdf : GeoDataFrame
        GeoDataFrame with basin geometries
    all_ratio_data : dict
        Nested dictionary with product names as keys and basin ratio dicts as values
    output_dir : str
        Directory to save the figure
    year_label : str
        Label for the year range (e.g., 'WY2001-2015', 'CY2000-2018')
    """
    products = list(all_ratio_data.keys())
    n_products = len(products)

    # Calculate grid dimensions (4x4 for 16 products)
    n_cols = 4
    n_rows = (n_products + n_cols - 1) // n_cols

    # Size each panel to roughly match the basins' aspect ratio
    minx, miny, maxx, maxy = basins_gdf.total_bounds
    aspect = max((maxx - minx) / max(maxy - miny, 1e-9), 0.25)
    panel_w = 4
    panel_h = panel_w / aspect
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(panel_w * n_cols, panel_h * n_rows))
    axes = axes.flatten() if n_products > 1 else [axes]
    
    # Reduce gaps between subplots
    plt.subplots_adjust(wspace=0.05, hspace=0.15)
    
    # Common colormap settings centered at 1.0 (AET = PPT)
    cmap = plt.cm.RdYlBu_r
    norm = mcolors.TwoSlopeNorm(vmin=0, vcenter=1.0, vmax=1.5)
    
    for idx, (product, ratio_data) in enumerate(all_ratio_data.items()):
        ax = axes[idx]
        
        # Add ratio values to GeoDataFrame
        basins_plot = basins_gdf.copy()
        basins_plot['et_ppt_ratio'] = basins_plot['BasinName'].map(ratio_data)
        
        # Plot basins
        basins_plot.plot(column='et_ppt_ratio', ax=ax, cmap=cmap, norm=norm,
                        edgecolor='black', linewidth=0.3, legend=False,
                        missing_kwds={'color': 'lightgray'})
        
        ax.set_title(product, fontsize=11, fontweight='bold')
        # Fit tightly around the basins with a small margin
        minx, miny, maxx, maxy = basins_gdf.total_bounds
        dx = (maxx - minx) * 0.05
        dy = (maxy - miny) * 0.05
        ax.set_xlim(minx - dx, maxx + dx)
        ax.set_ylim(miny - dy, maxy + dy)
        ax.set_aspect('equal')
        # Disable lat/long grids and labels
        ax.set_axis_off()
    
    # Hide unused subplots
    for idx in range(n_products, len(axes)):
        axes[idx].set_visible(False)
    
    # Add common colorbar
    fig.subplots_adjust(right=0.88)
    cbar_ax = fig.add_axes([0.90, 0.15, 0.02, 0.7])
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=cbar_ax, label='AET/PPT Ratio')
    cbar.ax.axhline(y=1.0, color='black', linewidth=2, linestyle='--')
    
    fig.suptitle(f'Mean Annual AET/PPT Ratio by ET Product ({year_label})', 
                 fontsize=14, fontweight='bold', y=0.95)
    
    plt.savefig(os.path.join(output_dir, 'all_products_et_ppt_ratio_maps.png'), 
                dpi=600, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'all_products_et_ppt_ratio_maps.pdf'), 
                dpi=600, bbox_inches='tight')
    plt.close()


def plot_iwv_bar_comparison(all_basin_data, output_dir, year_label='WY2001-2015',
                                   region_label='IWV', filename_prefix='iwv_et_comparison'):
    """
    Create a summary bar chart comparing all ET products summed across basins.

    Parameters:
    -----------
    all_basin_data : DataFrame
        DataFrame with basin-level mean annual ET for all products
    output_dir : str
        Directory to save the figure
    year_label : str
        Label for the year range (e.g., 'WY2001-2015')
    region_label : str
        Label for the region (used in title)
    filename_prefix : str
        Output file name prefix
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Calculate IWV basin totals
    products = ['USGS BCM/Flint', 'USGS BCM/Reitz', 'USGS SSEBop-WB/Reitz', 'USGS Reitz Ensemble',
                'MOD16', 'PMLv2', 'SSEBop VIIRS',
                'SSEBop MODIS', 'OpenET Ensemble', 'OpenET SSEBop',
                'OpenET eeMETRIC', 'OpenET DisALEXI', 'OpenET geeSEBAL',
                'OpenET PT-JPL', 'WLDAS', 'TerraClimate']

    product_cols = [col for col in all_basin_data.columns if col in products]
    
    # Sum across all basins (acre-ft)
    totals = all_basin_data[product_cols].sum()
    
    colors = [ET_COLORS.get(p, '#333333') for p in totals.index]
    bars = ax.bar(range(len(totals)), totals.values / 1e6, color=colors, 
                  edgecolor='black', linewidth=0.5)
    
    # Add value labels
    for bar, val in zip(bars, totals.values / 1e6):
        ax.annotate(f'{val:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, val),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)
    
    ax.set_xticks(range(len(totals)))
    ax.set_xticklabels(totals.index, rotation=45, ha='right')
    ax.set_ylabel('Total AET (Million acre-ft/yr)')
    ax.set_title(f'{region_label} Total AET by Product\n(Sum of All Basins, {year_label} Mean)')

    # Secondary y-axis in million m^3/yr
    # (Million acre-ft) * (1.23348e9 m^3 / Million acre-ft) / 1e6 (to Mm^3)
    # = Million acre-ft * 1233.48 = Million m^3
    ax_m3 = ax.twinx()
    lo, hi = ax.get_ylim()
    ax_m3.set_ylim(lo * ACRE_FT_TO_M3, hi * ACRE_FT_TO_M3)
    ax_m3.set_ylabel('Total AET (Million m³/yr)')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{filename_prefix}.png'),
                dpi=600, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, f'{filename_prefix}.pdf'),
                dpi=600, bbox_inches='tight')
    plt.close()


# ============================================================================
# Statistical Analysis Functions
# ============================================================================

def compute_difference_metrics(reference, comparison):
    """
    Compute difference metrics between reference and comparison arrays.
    
    Parameters:
    -----------
    reference : array-like
        Reference values (e.g., USGS Reitz)
    comparison : array-like
        Comparison values (e.g., GEE products)
    
    Returns:
    --------
    dict : Dictionary with MBD, RMSD, MAD, and percentage versions
    """
    ref = np.array(reference)
    comp = np.array(comparison)
    
    # Remove NaN pairs
    valid_mask = ~(np.isnan(ref) | np.isnan(comp))
    ref = ref[valid_mask]
    comp = comp[valid_mask]
    
    if len(ref) == 0:
        return {'MBD': np.nan, 'RMSD': np.nan, 'MAD': np.nan, 
                'MBD_pct': np.nan, 'RMSD_pct': np.nan, 'MAD_pct': np.nan, 'n': 0}
    
    # Compute differences
    diff = comp - ref
    
    # Mean Bias Difference (MBD)
    mbd = np.mean(diff)
    
    # Root Mean Square Difference (RMSD)
    rmsd = np.sqrt(np.mean(diff ** 2))
    
    # Mean Absolute Difference (MAD)
    mad = np.mean(np.abs(diff))
    
    # Percentage versions (relative to reference mean)
    ref_mean = np.mean(ref)
    if ref_mean != 0:
        mbd_pct = (mbd / ref_mean) * 100
        rmsd_pct = (rmsd / ref_mean) * 100
        mad_pct = (mad / ref_mean) * 100
    else:
        mbd_pct = rmsd_pct = mad_pct = np.nan
    
    return {
        'MBD': mbd,
        'RMSD': rmsd,
        'MAD': mad,
        'MBD_pct': mbd_pct,
        'RMSD_pct': rmsd_pct,
        'MAD_pct': mad_pct,
        'n': len(ref)
    }


def compute_correlations(reference, comparison):
    """
    Compute Pearson and Spearman correlations.
    
    Parameters:
    -----------
    reference : array-like
        Reference values
    comparison : array-like
        Comparison values
    
    Returns:
    --------
    dict : Dictionary with Pearson r, Spearman rho, and p-values
    """
    from scipy import stats
    
    ref = np.array(reference)
    comp = np.array(comparison)
    
    # Remove NaN pairs
    valid_mask = ~(np.isnan(ref) | np.isnan(comp))
    ref = ref[valid_mask]
    comp = comp[valid_mask]
    
    if len(ref) < 3:
        return {'pearson_r': np.nan, 'pearson_p': np.nan,
                'spearman_rho': np.nan, 'spearman_p': np.nan, 'n': len(ref)}
    
    # Pearson correlation
    pearson_r, pearson_p = stats.pearsonr(ref, comp)
    
    # Spearman correlation
    spearman_rho, spearman_p = stats.spearmanr(ref, comp)
    
    return {
        'pearson_r': pearson_r,
        'pearson_p': pearson_p,
        'spearman_rho': spearman_rho,
        'spearman_p': spearman_p,
        'n': len(ref)
    }


def compute_all_metrics(summary_df, reference_col='USGS BCM/Reitz'):
    """
    Compute all error metrics and correlations for each product vs reference.
    
    Parameters:
    -----------
    summary_df : DataFrame
        Summary DataFrame with ET values for all products
    reference_col : str
        Name of the reference column
    
    Returns:
    --------
    DataFrame : Metrics for each product
    """
    products = ['USGS BCM/Flint', 'USGS SSEBop-WB/Reitz', 'USGS Reitz Ensemble',
                'MOD16', 'PMLv2', 'SSEBop VIIRS', 'SSEBop MODIS',
                'OpenET Ensemble', 'OpenET SSEBop', 'OpenET eeMETRIC',
                'OpenET DisALEXI', 'OpenET geeSEBAL', 'OpenET PT-JPL',
                'WLDAS', 'TerraClimate']

    metrics_list = []
    ref_values = summary_df[reference_col].values
    
    for product in products:
        if product not in summary_df.columns:
            continue
        
        comp_values = summary_df[product].values
        
        # Compute difference metrics
        diff_metrics = compute_difference_metrics(ref_values, comp_values)
        
        # Compute correlations
        corr_metrics = compute_correlations(ref_values, comp_values)
        
        metrics_list.append({
            'Product': product,
            'MBD (ac-ft)': diff_metrics['MBD'],
            'RMSD (ac-ft)': diff_metrics['RMSD'],
            'MAD (ac-ft)': diff_metrics['MAD'],
            'MBD (%)': diff_metrics['MBD_pct'],
            'RMSD (%)': diff_metrics['RMSD_pct'],
            'MAD (%)': diff_metrics['MAD_pct'],
            'Pearson r': corr_metrics['pearson_r'],
            'Pearson p': corr_metrics['pearson_p'],
            'Spearman ρ': corr_metrics['spearman_rho'],
            'Spearman p': corr_metrics['spearman_p'],
            'N': diff_metrics['n']
        })
    
    return pd.DataFrame(metrics_list)


def compute_product_agreement(summary_df):
    """
    Compute agreement statistics across all products for each basin.
    
    Parameters:
    -----------
    summary_df : DataFrame
        Summary DataFrame with ET values for all products
    
    Returns:
    --------
    DataFrame : Agreement statistics for each basin
    """
    products = ['USGS BCM/Flint', 'USGS BCM/Reitz', 'USGS SSEBop-WB/Reitz', 'USGS Reitz Ensemble',
                'MOD16', 'PMLv2', 'SSEBop VIIRS', 'SSEBop MODIS',
                'OpenET Ensemble', 'OpenET SSEBop', 'OpenET eeMETRIC',
                'OpenET DisALEXI', 'OpenET geeSEBAL', 'OpenET PT-JPL',
                'WLDAS', 'TerraClimate']

    product_cols = [col for col in products if col in summary_df.columns]
    
    agreement_data = []
    for idx, row in summary_df.iterrows():
        values = row[product_cols].values.astype(float)
        valid_values = values[~np.isnan(values)]
        
        if len(valid_values) > 1:
            agreement_data.append({
                'BasinName': row['BasinName'],
                'Mean_ET': np.mean(valid_values),
                'Std_ET': np.std(valid_values),
                'CV_ET': np.std(valid_values) / np.mean(valid_values) * 100 if np.mean(valid_values) > 0 else np.nan,
                'Range_ET': np.max(valid_values) - np.min(valid_values),
                'Min_ET': np.min(valid_values),
                'Max_ET': np.max(valid_values),
                'N_Products': len(valid_values)
            })
    
    return pd.DataFrame(agreement_data)


def plot_difference_metrics_bar(metrics_df, output_dir):
    """
    Create bar charts of difference metrics for all products.
    
    Parameters:
    -----------
    metrics_df : DataFrame
        DataFrame with error metrics
    output_dir : str
        Directory to save figures
    """
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    products = metrics_df['Product'].values
    colors = [ET_COLORS.get(p, '#333333') for p in products]
    x = np.arange(len(products))
    
    # MBD plot
    ax = axes[0]
    bars = ax.bar(x, metrics_df['MBD (%)'].values, color=colors, edgecolor='black', linewidth=0.5)
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(products, rotation=45, ha='right')
    ax.set_ylabel('Mean Bias Difference (%)')
    ax.set_title('MBD vs USGS BCM/Reitz')
    
    # RMSD plot
    ax = axes[1]
    bars = ax.bar(x, metrics_df['RMSD (%)'].values, color=colors, edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(products, rotation=45, ha='right')
    ax.set_ylabel('Root Mean Square Difference (%)')
    ax.set_title('RMSD vs USGS BCM/Reitz')
    
    # MAD plot
    ax = axes[2]
    bars = ax.bar(x, metrics_df['MAD (%)'].values, color=colors, edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(products, rotation=45, ha='right')
    ax.set_ylabel('Mean Absolute Difference (%)')
    ax.set_title('MAD vs USGS BCM/Reitz')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'difference_metrics_bar.png'), dpi=600, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'difference_metrics_bar.pdf'), dpi=600, bbox_inches='tight')
    plt.close()


def plot_correlation_bar(metrics_df, output_dir):
    """
    Create bar chart of correlation coefficients.
    
    Parameters:
    -----------
    metrics_df : DataFrame
        DataFrame with correlation metrics
    output_dir : str
        Directory to save figures
    """
    fig, ax = plt.subplots(figsize=(10, 5))
    
    products = metrics_df['Product'].values
    colors = [ET_COLORS.get(p, '#333333') for p in products]
    x = np.arange(len(products))
    width = 0.35
    
    # Pearson and Spearman side by side
    bars1 = ax.bar(x - width/2, metrics_df['Pearson r'].values, width, 
                   color=colors, edgecolor='black', linewidth=0.5, label='Pearson r')
    bars2 = ax.bar(x + width/2, metrics_df['Spearman ρ'].values, width, 
                   color=colors, edgecolor='black', linewidth=0.5, alpha=0.6, 
                   hatch='///', label='Spearman ρ')
    
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax.axhline(y=0.7, color='green', linestyle='--', linewidth=1, alpha=0.7)
    ax.axhline(y=-0.7, color='green', linestyle='--', linewidth=1, alpha=0.7)
    
    ax.set_xticks(x)
    ax.set_xticklabels(products, rotation=45, ha='right')
    ax.set_ylabel('Correlation Coefficient')
    ax.set_title('Correlation with USGS BCM/Reitz ET')
    ax.set_ylim(-1, 1)
    ax.legend(loc='lower right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'correlation_bar.png'), dpi=600, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'correlation_bar.pdf'), dpi=600, bbox_inches='tight')
    plt.close()


def plot_scatter_comparison(summary_df, output_dir, reference_col='USGS BCM/Reitz'):
    """
    Create scatter plots comparing each product to reference.
    
    Parameters:
    -----------
    summary_df : DataFrame
        Summary DataFrame with ET values
    output_dir : str
        Directory to save figures
    reference_col : str
        Name of reference column
    """
    products = ['USGS BCM/Flint', 'USGS SSEBop-WB/Reitz', 'USGS Reitz Ensemble',
                'MOD16', 'PMLv2', 'SSEBop VIIRS', 'SSEBop MODIS',
                'OpenET Ensemble', 'OpenET SSEBop', 'OpenET eeMETRIC',
                'OpenET DisALEXI', 'OpenET geeSEBAL', 'OpenET PT-JPL',
                'WLDAS', 'TerraClimate']

    available_products = [p for p in products if p in summary_df.columns]
    n_products = len(available_products)

    n_cols = 4
    n_rows = (n_products + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(12, 4 * n_rows))
    axes = axes.flatten()
    
    ref_values = summary_df[reference_col].values
    max_val = np.nanmax(ref_values) * 1.1
    
    for idx, product in enumerate(available_products):
        ax = axes[idx]
        comp_values = summary_df[product].values
        
        # Get color for this product
        color = ET_COLORS.get(product, '#333333')
        
        # Scatter plot
        ax.scatter(ref_values, comp_values, alpha=0.6, s=20, c=color, edgecolor='none')
        
        # 1:1 line
        ax.plot([0, max_val], [0, max_val], 'k--', linewidth=1, label='1:1')
        
        # Fit line
        valid_mask = ~(np.isnan(ref_values) | np.isnan(comp_values))
        if np.sum(valid_mask) > 2:
            z = np.polyfit(ref_values[valid_mask], comp_values[valid_mask], 1)
            p = np.poly1d(z)
            ax.plot([0, max_val], [p(0), p(max_val)], color=color, linewidth=1.5, 
                   label=f'Fit: y={z[0]:.2f}x+{z[1]:.0f}')
        
        # Compute metrics for annotation
        metrics = compute_difference_metrics(ref_values, comp_values)
        corr = compute_correlations(ref_values, comp_values)
        
        ax.text(0.05, 0.95, f"r = {corr['pearson_r']:.3f}\nMBD = {metrics['MBD_pct']:.1f}%\nRMSD = {metrics['RMSD_pct']:.1f}%",
               transform=ax.transAxes, fontsize=8, verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        ax.set_xlabel(f'{reference_col} (ac-ft/yr)')
        ax.set_ylabel(f'{product} (ac-ft/yr)')
        ax.set_title(product)
        ax.set_xlim(0, max_val)
        ax.set_ylim(0, np.nanmax(comp_values) * 1.1 if np.any(~np.isnan(comp_values)) else max_val)
        ax.legend(loc='lower right', fontsize=7)
    
    # Hide unused subplots
    for idx in range(n_products, len(axes)):
        axes[idx].set_visible(False)
    
    plt.suptitle(f'ET Product Comparison with {reference_col}', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'scatter_comparison.png'), dpi=600, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'scatter_comparison.pdf'), dpi=600, bbox_inches='tight')
    plt.close()


def plot_agreement_map(basins_gdf, agreement_df, output_dir):
    """
    Create maps showing product agreement (CV) across basins.
    
    Parameters:
    -----------
    basins_gdf : GeoDataFrame
        Basin geometries
    agreement_df : DataFrame
        Agreement statistics
    output_dir : str
        Directory to save figures
    """
    # Size the panels to roughly match the basins' aspect ratio
    minx, miny, maxx, maxy = basins_gdf.total_bounds
    aspect = max((maxx - minx) / max(maxy - miny, 1e-9), 0.25)
    panel_w = 6
    panel_h = panel_w / aspect
    fig, axes = plt.subplots(1, 2, figsize=(panel_w * 2, panel_h))

    # Merge agreement data with basins
    basins_plot = basins_gdf.merge(agreement_df, on='BasinName', how='left')

    dx = (maxx - minx) * 0.05
    dy = (maxy - miny) * 0.05

    # Plot 1: Coefficient of Variation (CV)
    ax = axes[0]
    cmap = plt.cm.YlOrRd
    norm = mcolors.Normalize(vmin=0, vmax=100)
    basins_plot.plot(column='CV_ET', ax=ax, cmap=cmap, norm=norm,
                    edgecolor='black', linewidth=0.3, legend=False,
                    missing_kwds={'color': 'lightgray'})
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, shrink=0.7, label='CV (%)')
    ax.set_title('Product Disagreement\n(Coefficient of Variation)', fontsize=11, fontweight='bold')
    ax.set_xlim(minx - dx, maxx + dx)
    ax.set_ylim(miny - dy, maxy + dy)
    ax.set_aspect('equal')
    ax.set_axis_off()

    # Plot 2: Range
    ax = axes[1]
    range_max = np.nanpercentile(basins_plot['Range_ET'].values, 95)
    norm = mcolors.Normalize(vmin=0, vmax=range_max)
    basins_plot.plot(column='Range_ET', ax=ax, cmap=cmap, norm=norm,
                    edgecolor='black', linewidth=0.3, legend=False,
                    missing_kwds={'color': 'lightgray'})
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, shrink=0.7, label='Range (ac-ft/yr)')
    ax.set_title('Product Disagreement\n(Max - Min ET)', fontsize=11, fontweight='bold')
    ax.set_xlim(minx - dx, maxx + dx)
    ax.set_ylim(miny - dy, maxy + dy)
    ax.set_aspect('equal')
    ax.set_axis_off()
    
    plt.suptitle('Inter-Product Agreement Analysis', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'product_agreement_maps.png'), dpi=600, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'product_agreement_maps.pdf'), dpi=600, bbox_inches='tight')
    plt.close()


def plot_metrics_heatmap(metrics_df, output_dir):
    """
    Create heatmap of all metrics for easy comparison.
    
    Parameters:
    -----------
    metrics_df : DataFrame
        DataFrame with all metrics
    output_dir : str
        Directory to save figures
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Select numeric columns for heatmap
    display_cols = ['MBD (%)', 'RMSD (%)', 'MAD (%)', 'Pearson r', 'Spearman ρ']
    heatmap_data = metrics_df.set_index('Product')[display_cols].astype(float)
    
    # Normalize for visualization (different scales)
    # Create a diverging colormap for the heatmap
    im = ax.imshow(heatmap_data.values, cmap='RdYlGn_r', aspect='auto')
    
    # Set ticks
    ax.set_xticks(np.arange(len(display_cols)))
    ax.set_yticks(np.arange(len(heatmap_data.index)))
    ax.set_xticklabels(display_cols, rotation=45, ha='right')
    ax.set_yticklabels(heatmap_data.index)
    
    # Add text annotations
    for i in range(len(heatmap_data.index)):
        for j in range(len(display_cols)):
            val = heatmap_data.iloc[i, j]
            text_color = 'white' if abs(val) > 50 or (j >= 3 and abs(val) < 0.5) else 'black'
            text = f'{val:.1f}' if j < 3 else f'{val:.2f}'
            ax.text(j, i, text, ha='center', va='center', color=text_color, fontsize=9)
    
    ax.set_title('Difference Metrics and Correlations vs USGS BCM/Reitz', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'metrics_heatmap.png'), dpi=600, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'metrics_heatmap.pdf'), dpi=600, bbox_inches='tight')
    plt.close()


# ============================================================================
# Main Processing Functions
# ============================================================================

def process_basin(basin_gdf, years, use_gee=True, use_parallel=True, gee_scale=200):
    """
    Process a single basin: extract all ET products and compute statistics.
    Water-year mode only. Reitz ET comes from local 200 m ADF rasters; all
    other products come directly from GEE at monthly scale.

    Parameters:
    -----------
    basin_gdf : GeoDataFrame
        Single-row GeoDataFrame for the basin
    years : list
        List of water years to process
    use_gee : bool
        Whether to extract data from GEE
    use_parallel : bool
        Whether to use parallel processing within basin
    gee_scale : int
        Scale (resolution in meters) for GEE reduceRegion

    Returns:
    --------
    dict : Dictionary containing basin statistics
    """
    basin_name = basin_gdf['BasinName'].values[0]
    area_acres = calculate_basin_area_acres(basin_gdf)

    results = {
        'basin_name': basin_name,
        'area_acres': area_acres,
    }

    # Extract USGS BCM/Flint from local 270 m ASC rasters (Flint et al., 2021)
    bcm_aet = extract_bcm_aet_for_basin(basin_gdf, years, use_parallel=use_parallel)
    results['bcm_aet_mm'] = bcm_aet
    results['bcm_aet_mean_mm'] = np.nanmean(bcm_aet)

    # Extract USGS Reitz ET from local 200 m ADF rasters (parallelized)
    reitz_et = extract_reitz_et_for_basin(basin_gdf, years, use_parallel=use_parallel)
    results['reitz_et_mm'] = reitz_et
    results['reitz_et_mean_mm'] = np.nanmean(reitz_et)

    if use_gee:
        # Extract all GEE products (parallelized with Dask)
        gee_data = extract_all_et_products(basin_gdf, years, use_parallel=use_parallel,
                                           scale=gee_scale)

        # Store time series
        results['gee_timeseries'] = gee_data

        # Calculate means
        for col in gee_data.columns:
            if col != 'year':
                results[f'{col}_mean_mm'] = np.nanmean(gee_data[col])

    return results


def _load_existing_basin_data(basin_name, years, output_dir, area_acres):
    """
    Load existing basin data from CSV files if available.
    
    Parameters:
    -----------
    basin_name : str
        Name of the basin
    years : list
        List of years expected
    output_dir : str
        Base output directory
    area_acres : float
        Basin area in acres
    
    Returns:
    --------
    dict or None : Results dictionary if data exists, None otherwise
    """
    basin_dir_name = basin_name.replace(' ', '_').replace('/', '_')
    basin_output_dir = os.path.join(output_dir, 'basins', basin_dir_name)
    csv_path = os.path.join(basin_output_dir, 'et_timeseries.csv')
    
    if not os.path.exists(csv_path):
        return None
    
    try:
        ts_data = pd.read_csv(csv_path)
        
        # Verify years match
        if 'year' not in ts_data.columns:
            return None
        csv_years = ts_data['year'].tolist()
        if csv_years != list(years):
            print(f"  Year mismatch for {basin_name}, will re-extract")
            return None
        
        # Reconstruct results dictionary
        results = {
            'basin_name': basin_name,
            'area_acres': area_acres,
        }
        
        # Map display names back to internal keys
        display_to_key = {
            'MOD16': 'mod16',
            'PMLv2': 'pmlv2',
            'SSEBop VIIRS': 'ssebop_viirs',
            'SSEBop MODIS': 'ssebop_modis',
            'OpenET Ensemble': 'openet',
            'OpenET SSEBop': 'openet_ssebop',
            'OpenET eeMETRIC': 'openet_eemetric',
            'OpenET DisALEXI': 'openet_disalexi',
            'OpenET geeSEBAL': 'openet_geesebal',
            'OpenET PT-JPL': 'openet_ptjpl',
            'WLDAS': 'wldas',
            'TerraClimate': 'terraclimate',
            'USGS SSEBop-WB/Reitz': 'reitz_ssebop_wb',
            'USGS Reitz Ensemble': 'reitz_ensemble',
            'PRISM PPT': 'prism_ppt',
            # Legacy column names from earlier runs (pre-rename). The local
            # Reitz ADF column ('USGS Reitz') is handled separately below.
            'Reitz Ensemble': 'reitz_ensemble',
            'Reitz SSEBop-WB': 'reitz_ssebop_wb',
        }

        # Extract USGS BCM/Flint data if present in CSV
        if 'USGS BCM/Flint' in ts_data.columns:
            bcm_values = ts_data['USGS BCM/Flint'].values
            results['bcm_aet_mm'] = list(bcm_values)
            results['bcm_aet_mean_mm'] = np.nanmean(bcm_values)
        else:
            results['bcm_aet_mm'] = [np.nan] * len(years)
            results['bcm_aet_mean_mm'] = np.nan

        # Extract USGS Reitz data (prefer new column name, fall back to legacy)
        reitz_col = ('USGS BCM/Reitz' if 'USGS BCM/Reitz' in ts_data.columns
                     else ('USGS Reitz' if 'USGS Reitz' in ts_data.columns else None))
        if reitz_col is not None:
            reitz_values = ts_data[reitz_col].values
            results['reitz_et_mm'] = list(reitz_values)
            results['reitz_et_mean_mm'] = np.nanmean(reitz_values)
        else:
            results['reitz_et_mm'] = [np.nan] * len(years)
            results['reitz_et_mean_mm'] = np.nan
        
        # Build GEE timeseries DataFrame
        gee_data = {'year': years}
        for display_name, key in display_to_key.items():
            if display_name in ts_data.columns:
                values = ts_data[display_name].values
                gee_data[key] = list(values)
                results[f'{key}_mean_mm'] = np.nanmean(values)
        
        results['gee_timeseries'] = pd.DataFrame(gee_data)
        
        return results
        
    except Exception as e:
        print(f"  Error loading existing data for {basin_name}: {e}")
        return None


def _process_basin_wrapper(args_tuple):
    """
    Wrapper function for parallel basin processing.
    Unpacks arguments and calls process_basin.
    Loads existing CSV data if available to skip GEE extraction.
    """
    (basin_gdf, years, use_gee, use_parallel, output_dir, gee_scale, year_label) = args_tuple
    basin_name = basin_gdf['BasinName'].values[0]

    # Calculate area for potential CSV loading
    basin_proj = basin_gdf.to_crs('EPSG:5070')
    area_m2 = basin_proj.geometry.values[0].area
    area_acres = area_m2 * SQ_M_TO_ACRES

    # Try to load existing data first
    existing_results = _load_existing_basin_data(basin_name, years, output_dir, area_acres)
    if existing_results is not None:
        print(f"  Loaded existing data for {basin_name}")
        # Still regenerate plots with loaded data to reflect current product list
        results = existing_results
    else:
        try:
            results = process_basin(basin_gdf, years, use_gee=use_gee,
                                   use_parallel=use_parallel, gee_scale=gee_scale)
        except Exception as e:
            print(f"\nError processing {basin_name}: {e}")
            return None
    
    try:
        # Create individual basin plots
        basin_output_dir = os.path.join(output_dir, 'basins', 
                                        basin_name.replace(' ', '_').replace('/', '_'))
        os.makedirs(basin_output_dir, exist_ok=True)
        
        # Prepare ET data for bar chart
        et_data = {}
        # USGS BCM/Flint first
        if not np.isnan(results.get('bcm_aet_mean_mm', np.nan)):
            et_data['USGS BCM/Flint'] = results['bcm_aet_mean_mm']
        # Then USGS Reitz if available
        if not np.isnan(results['reitz_et_mean_mm']):
            et_data['USGS BCM/Reitz'] = results['reitz_et_mean_mm']
        
        if 'gee_timeseries' in results:
            # Group the three USGS Reitz products first (BCM/Reitz already
            # added above), then GEE products.
            product_mapping = {
                'reitz_ssebop_wb': 'USGS SSEBop-WB/Reitz',
                'reitz_ensemble': 'USGS Reitz Ensemble',
                'mod16': 'MOD16',
                'pmlv2': 'PMLv2',
                'ssebop_viirs': 'SSEBop VIIRS',
                'ssebop_modis': 'SSEBop MODIS',
                'openet': 'OpenET Ensemble',
                'openet_ssebop': 'OpenET SSEBop',
                'openet_eemetric': 'OpenET eeMETRIC',
                'openet_disalexi': 'OpenET DisALEXI',
                'openet_geesebal': 'OpenET geeSEBAL',
                'openet_ptjpl': 'OpenET PT-JPL',
                'wldas': 'WLDAS',
                'terraclimate': 'TerraClimate',
            }
            for key, name in product_mapping.items():
                if f'{key}_mean_mm' in results:
                    et_data[name] = results[f'{key}_mean_mm']
        
        # Plot bar chart with correct year label
        plot_basin_et_barchart(
            basin_name, et_data, results['area_acres'],
            os.path.join(basin_output_dir, 'et_barchart.png'),
            year_label=year_label
        )
        
        # Plot time series if GEE data available (from fresh extraction or loaded CSV)
        if 'gee_timeseries' in results:
            ts_data = results['gee_timeseries'].copy()
            
            # Add USGS BCM/Flint to time series if valid data available
            bcm_aet_list = results.get('bcm_aet_mm', [])
            if bcm_aet_list and not all(np.isnan(v) for v in bcm_aet_list):
                ts_data['USGS BCM/Flint'] = bcm_aet_list

            # Add USGS Reitz (200 m ADF) to time series if valid data available
            reitz_et_list = results.get('reitz_et_mm', [])
            if reitz_et_list and not all(np.isnan(v) for v in reitz_et_list):
                ts_data['USGS BCM/Reitz'] = reitz_et_list

            # Rename columns to display names for proper color mapping
            column_rename = {
                'mod16': 'MOD16',
                'pmlv2': 'PMLv2',
                'ssebop_viirs': 'SSEBop VIIRS',
                'ssebop_modis': 'SSEBop MODIS',
                'openet': 'OpenET Ensemble',
                'openet_ssebop': 'OpenET SSEBop',
                'openet_eemetric': 'OpenET eeMETRIC',
                'openet_disalexi': 'OpenET DisALEXI',
                'openet_geesebal': 'OpenET geeSEBAL',
                'openet_ptjpl': 'OpenET PT-JPL',
                'wldas': 'WLDAS',
                'terraclimate': 'TerraClimate',
                'reitz_ssebop_wb': 'USGS SSEBop-WB/Reitz',
                'reitz_ensemble': 'USGS Reitz Ensemble',
                'prism_ppt': 'PRISM PPT'
            }
            ts_data = ts_data.rename(columns=column_rename)
            
            # Save time series data to CSV
            ts_data.to_csv(os.path.join(basin_output_dir, 'et_timeseries.csv'), index=False)
            
            plot_multi_product_comparison(
                basin_name, ts_data,
                os.path.join(basin_output_dir, 'et_timeseries.png'),
                year_label=year_label
            )
        
        return results
    except Exception as e:
        print(f"\nError processing {basin_name}: {e}")
        return None


def derive_whole_iwv_results(all_results, summary_df, basins_gdf, output_dir, year_label):
    """
    Derive whole-IWV-basin statistics by aggregating sub-basin results.

    Per-basin values are in mm (time series) or ac-ft (summary). Whole-IWV:
      - ac-ft totals: summed across sub-basins (additive)
      - mm (per-year time series): area-weighted mean across sub-basins

    Parameters
    ----------
    all_results : list of dict
        Per-sub-basin result dicts from ``process_basin`` / CSV reload.
    summary_df : DataFrame
        Sub-basin summary with one row per sub-basin (ac-ft columns).
    basins_gdf : GeoDataFrame
        Sub-basin geometries used for the dissolved IWV outline plot.
    output_dir : str
        Where to write the whole-IWV outputs.
    year_label : str
        Year range label (e.g., 'WY2001-2015').
    """
    if summary_df is None or len(summary_df) == 0:
        return None

    os.makedirs(output_dir, exist_ok=True)
    print(f"\nDeriving whole-IWV results from {len(summary_df)} sub-basins...")

    # --- (1) Summary ac-ft totals across sub-basins -------------------------
    total_area = float(summary_df['Area_acres'].sum())
    product_cols = [c for c in summary_df.columns if c not in ('BasinName', 'Area_acres')]

    whole_row = {'BasinName': 'IWV (whole basin)', 'Area_acres': total_area}
    for col in product_cols:
        vals = summary_df[col].values.astype(float)
        if np.all(np.isnan(vals)):
            whole_row[col] = np.nan
        elif np.any(np.isnan(vals)):
            # Partial coverage would bias the total; flag as NaN
            whole_row[col] = np.nan
        else:
            whole_row[col] = float(np.sum(vals))

    iwv_summary = pd.DataFrame([whole_row])
    iwv_summary.to_csv(os.path.join(output_dir, 'iwv_whole_basin_et_summary.csv'), index=False)

    # Bar chart of IWV totals (a one-row DataFrame still works with
    # plot_iwv_bar_comparison since it sums across rows)
    plot_iwv_bar_comparison(
        iwv_summary, output_dir, year_label=year_label,
        region_label='Indian Wells Valley (whole)',
        filename_prefix='iwv_whole_basin_barchart'
    )

    # --- (2) Area-weighted annual time series (mm) --------------------------
    # Build area lookup from summary_df
    area_by_name = dict(zip(summary_df['BasinName'], summary_df['Area_acres']))

    years = None
    ts_accum = None   # dict: product -> np.array(sum of et_mm * area)
    area_accum = None  # dict: product -> np.array(sum of area where valid)

    for r in all_results:
        gee_ts = r.get('gee_timeseries')
        if gee_ts is None:
            continue
        area = float(area_by_name.get(r['basin_name'], np.nan))
        if np.isnan(area):
            continue
        if years is None:
            years = list(gee_ts['year'].values)
            ts_accum = {}
            area_accum = {}
        # GEE products
        for col in gee_ts.columns:
            if col == 'year':
                continue
            vals = gee_ts[col].values.astype(float)
            if col not in ts_accum:
                ts_accum[col] = np.zeros(len(years))
                area_accum[col] = np.zeros(len(years))
            valid = ~np.isnan(vals)
            ts_accum[col][valid] += vals[valid] * area
            area_accum[col][valid] += area
        # Local Reitz ET time series
        reitz_ts = r.get('reitz_et_mm')
        if reitz_ts is not None and len(reitz_ts) == len(years):
            vals = np.asarray(reitz_ts, dtype=float)
            if 'reitz_et' not in ts_accum:
                ts_accum['reitz_et'] = np.zeros(len(years))
                area_accum['reitz_et'] = np.zeros(len(years))
            valid = ~np.isnan(vals)
            ts_accum['reitz_et'][valid] += vals[valid] * area
            area_accum['reitz_et'][valid] += area

    if years is None:
        return iwv_summary

    ts_out = {'year': years}
    display_rename = {
        'mod16': 'MOD16', 'pmlv2': 'PMLv2',
        'ssebop_viirs': 'SSEBop VIIRS', 'ssebop_modis': 'SSEBop MODIS',
        'openet': 'OpenET Ensemble', 'openet_ssebop': 'OpenET SSEBop',
        'openet_eemetric': 'OpenET eeMETRIC', 'openet_disalexi': 'OpenET DisALEXI',
        'openet_geesebal': 'OpenET geeSEBAL', 'openet_ptjpl': 'OpenET PT-JPL',
        'wldas': 'WLDAS', 'terraclimate': 'TerraClimate',
        'reitz_ssebop_wb': 'USGS SSEBop-WB/Reitz', 'reitz_ensemble': 'USGS Reitz Ensemble',
        'prism_ppt': 'PRISM PPT', 'reitz_et': 'USGS BCM/Reitz',
    }
    for key, acc in ts_accum.items():
        denom = area_accum[key]
        with np.errstate(invalid='ignore', divide='ignore'):
            mean_mm = np.where(denom > 0, acc / denom, np.nan)
        ts_out[display_rename.get(key, key)] = mean_mm

    ts_df = pd.DataFrame(ts_out)
    ts_df.to_csv(os.path.join(output_dir, 'iwv_whole_basin_timeseries.csv'), index=False)

    # Time series plot (re-use existing multi-product plot)
    plot_multi_product_comparison(
        'IWV (whole basin)', ts_df,
        os.path.join(output_dir, 'iwv_whole_basin_timeseries.png'),
        year_label=year_label
    )

    print(f"  Saved whole-IWV outputs to: {output_dir}")
    return iwv_summary


def main():
    """Main function to run the ET evaluation."""
    parser = argparse.ArgumentParser(description='Basin-level ET Evaluation')
    parser.add_argument('--test', action='store_true', help='Run in test mode (2 basins only)')
    parser.add_argument('--basins', type=int, default=None, help='Number of basins to process')
    parser.add_argument('--no-parallel', action='store_true', help='Disable parallel processing')
    parser.add_argument('--workers', type=int, default=N_WORKERS, help=f'Number of parallel workers (default: {N_WORKERS})')
    parser.add_argument('--basin-set', choices=['calibration', 'subbasin', 'both'],
                        default='both',
                        help="Which basin set(s) to process: 'calibration' "
                             "(IWV_calibrationBasin), 'subbasin' (IWV_SubBasin), "
                             "or 'both' (default).")
    args = parser.parse_args()

    print("=" * 60)
    print("Basin-level ET Evaluation (Dask Parallelized)")
    print("Water-year mode, GEE ingestion for all non-Reitz products")
    print("=" * 60)

    output_dir_template = OUTPUT_DIR_TEMPLATE

    # Determine which basin set(s) to process
    if args.basin_set == 'both':
        selected_basin_sets = ['calibration', 'subbasin']
    else:
        selected_basin_sets = [args.basin_set]
    print(f"\nBasin set(s) to process: {', '.join(selected_basin_sets)}")

    # Store per-set data for combined maps (populated inside the loop)
    basin_set_gdfs = {}       # basin_set_key -> GeoDataFrame
    basin_set_ratios = {}     # basin_set_key -> {product: {basin: ratio}}

    # Run the pipeline once per selected basin set
    for basin_set_key in selected_basin_sets:
        basins_path, basin_name_col = BASIN_SETS[basin_set_key]
        output_dir = output_dir_template.format(set=basin_set_key)

        print("\n" + "#" * 60)
        print(f"# Basin set: {basin_set_key}")
        print(f"# Shapefile: {basins_path}")
        print(f"# Name column: {basin_name_col}")
        print("#" * 60)

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        print(f"Output directory: {output_dir}")

        # Load basins early to check for existing data
        print(f"\nLoading basins ({basin_set_key})...")
        basins_gdf = load_basins(basins_path, name_col=basin_name_col)
        print(f"Loaded {len(basins_gdf)} basins")
    
        # Determine number of basins to process
        if args.test:
            basins_to_process = basins_gdf.head(2)
            print("\n*** TEST MODE: Processing only 2 basins ***\n")
        elif args.basins:
            basins_to_process = basins_gdf.head(args.basins)
            print(f"\n*** Processing {args.basins} basins ***\n")
        else:
            basins_to_process = basins_gdf
    
        # Check how many basins have existing CSV data
        def _check_existing_csv(basin_name):
            basin_dir_name = basin_name.replace(' ', '_').replace('/', '_')
            csv_path = os.path.join(output_dir, 'basins', basin_dir_name, 'et_timeseries.csv')
            return os.path.exists(csv_path)
    
        existing_count = sum(1 for bn in basins_to_process['BasinName'] if _check_existing_csv(bn))
        total_basins = len(basins_to_process)
        print(f"Found existing CSV data for {existing_count}/{total_basins} basins")

        # Initialize GEE
        if not initialize_gee():
            print("Running in local-only mode (USGS BCM/Reitz ET only)")
            use_gee = False
        else:
            use_gee = True

        # Water-year mode only; 200 m scale to match the local Reitz rasters
        gee_scale = 200
        years_to_process = WATER_YEARS
        year_label = f"WY{min(years_to_process)}-{max(years_to_process)}"
        print(f"Year mode: Water Year (Oct-Sep)")
        print(f"GEE extraction scale: {gee_scale}m")
        print(f"Year range: {year_label}")
    
        # Parallel processing settings
        use_parallel = not args.no_parallel
        n_workers = args.workers
    
        if use_parallel:
            print(f"\nParallel processing enabled with {n_workers} workers")
            print(f"GEE concurrent requests per basin: {GEE_MAX_WORKERS}")
        else:
            print("\nParallel processing disabled (sequential mode)")
    
        # Process basins with Dask parallelization at basin level
        # Note: We use sequential basin processing but parallel within-basin processing
        # to avoid overwhelming the GEE API with too many concurrent requests
        all_results = []
    
        if use_parallel and not use_gee:
            # For local-only processing, we can parallelize across basins
            print("\nProcessing basins in parallel (local data only)...")

            basin_args = [
                (basins_to_process.loc[[idx]], years_to_process, use_gee, True, output_dir,
                 gee_scale, year_label)
                for idx in basins_to_process.index
            ]

            # Create delayed tasks
            delayed_results = [delayed(_process_basin_wrapper)(args) for args in basin_args]

            # Compute with progress bar
            with ProgressBar():
                results = compute(*delayed_results, scheduler='threads', num_workers=n_workers)

            all_results = [r for r in results if r is not None]
        else:
            # For GEE processing, use sequential basin processing with parallel within-basin
            # to avoid GEE API rate limits and I/O conflicts
            for idx, row in tqdm(basins_to_process.iterrows(), total=len(basins_to_process),
                                 desc="Processing basins"):
                basin_gdf = basins_to_process.loc[[idx]]
                result = _process_basin_wrapper(
                    (basin_gdf, years_to_process, use_gee, use_parallel, output_dir,
                     gee_scale, year_label)
                )
                if result is not None:
                    all_results.append(result)
    
        # Create summary DataFrame
        print("\nCreating summary statistics...")
        summary_data = []
        for r in all_results:
            row = {
                'BasinName': r['basin_name'],
                'Area_acres': r['area_acres'],
            }
        
            # Add USGS BCM/Flint if valid data available (from local 270 m ASC rasters)
            if 'bcm_aet_mean_mm' in r and not np.isnan(r.get('bcm_aet_mean_mm', np.nan)):
                row['USGS BCM/Flint'] = mm_to_acre_ft(r['bcm_aet_mean_mm'], r['area_acres'])

            # Add USGS Reitz if valid data available (from local 200 m ADF rasters)
            if 'reitz_et_mean_mm' in r and not np.isnan(r['reitz_et_mean_mm']):
                row['USGS BCM/Reitz'] = mm_to_acre_ft(r['reitz_et_mean_mm'], r['area_acres'])

            # Add GEE products if available (from fresh extraction or loaded CSV).
            # Group the three USGS Reitz products first (BCM/Reitz added above),
            # then the remaining GEE products.
            product_mapping = {
                'reitz_ssebop_wb': 'USGS SSEBop-WB/Reitz',
                'reitz_ensemble': 'USGS Reitz Ensemble',
                'mod16': 'MOD16',
                'pmlv2': 'PMLv2',
                'ssebop_viirs': 'SSEBop VIIRS',
                'ssebop_modis': 'SSEBop MODIS',
                'openet': 'OpenET Ensemble',
                'openet_ssebop': 'OpenET SSEBop',
                'openet_eemetric': 'OpenET eeMETRIC',
                'openet_disalexi': 'OpenET DisALEXI',
                'openet_geesebal': 'OpenET geeSEBAL',
                'openet_ptjpl': 'OpenET PT-JPL',
                'wldas': 'WLDAS',
                'terraclimate': 'TerraClimate',
            }
            for key, name in product_mapping.items():
                if f'{key}_mean_mm' in r:
                    row[name] = mm_to_acre_ft(r.get(f'{key}_mean_mm', np.nan), r['area_acres'])
            summary_data.append(row)
    
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(os.path.join(output_dir, 'basin_et_summary.csv'), index=False)
    
        # Create IWV comparison plot with correct year label
        if len(summary_df) > 0:
            plot_iwv_bar_comparison(summary_df, output_dir, year_label=year_label)
    
        # Statistical Analysis: Difference Metrics and Correlations
        # Check if USGS Reitz data is available (either from ADF rasters or TIF files)
        has_reitz_data = 'USGS BCM/Reitz' in summary_df.columns and summary_df['USGS BCM/Reitz'].notna().any()
        # Check if GEE products are available (from fresh extraction or loaded CSV)
        has_gee_data = any(col in summary_df.columns for col in ['MOD16', 'PMLv2', 'OpenET Ensemble'])
    
        if has_gee_data and len(summary_df) > 3 and has_reitz_data:
            print("\nComputing difference metrics and correlations...")
        
            # Compute all metrics vs USGS Reitz
            metrics_df = compute_all_metrics(summary_df, reference_col='USGS BCM/Reitz')
            metrics_df.to_csv(os.path.join(output_dir, 'difference_metrics.csv'), index=False)
            print(f"  Saved difference metrics to difference_metrics.csv")
        
            # Plot difference metrics
            plot_difference_metrics_bar(metrics_df, output_dir)
            print(f"  Created difference_metrics_bar.png/pdf")
        
            # Plot correlations
            plot_correlation_bar(metrics_df, output_dir)
            print(f"  Created correlation_bar.png/pdf")
        
            # Plot scatter comparisons
            plot_scatter_comparison(summary_df, output_dir, reference_col='USGS BCM/Reitz')
            print(f"  Created scatter_comparison.png/pdf")
        
            # Plot metrics heatmap
            plot_metrics_heatmap(metrics_df, output_dir)
            print(f"  Created metrics_heatmap.png/pdf")
        
            # Product Agreement Analysis
            print("\nComputing product agreement statistics...")
            agreement_df = compute_product_agreement(summary_df)
            agreement_df.to_csv(os.path.join(output_dir, 'product_agreement.csv'), index=False)
            print(f"  Saved product agreement to product_agreement.csv")
        
            # Plot agreement maps
            plot_agreement_map(basins_gdf, agreement_df, output_dir)
            print(f"  Created product_agreement_maps.png/pdf")
        
            # Print summary statistics to console
            print("\n" + "-" * 60)
            print("Difference Metrics Summary (vs USGS BCM/Reitz):")
            print("-" * 60)
            print(metrics_df[['Product', 'MBD (%)', 'RMSD (%)', 'MAD (%)', 'Pearson r']].to_string(index=False))
        elif has_gee_data and len(summary_df) > 3 and not has_reitz_data:
            # No reference data available, only compute product agreement
            print("\nNote: USGS BCM/Reitz data not available for reference-based metrics.")
            print("Skipping difference metrics and correlations.")
        
            # Product Agreement Analysis (still possible without reference)
            print("\nComputing product agreement statistics...")
            agreement_df = compute_product_agreement(summary_df)
            agreement_df.to_csv(os.path.join(output_dir, 'product_agreement.csv'), index=False)
            print(f"  Saved product agreement to product_agreement.csv")
        
            # Plot agreement maps
            plot_agreement_map(basins_gdf, agreement_df, output_dir)
            print(f"  Created product_agreement_maps.png/pdf")
    
        # Create AET/PPT ratio maps (if GEE data available from extraction or loaded CSV)
        if has_gee_data:
            print("\nCreating AET/PPT ratio maps...")
            all_ratio_data = {}
        
            # Include USGS Reitz if data is available (from TIF files or ADF rasters)
            has_reitz = any(not np.isnan(r.get('reitz_et_mean_mm', np.nan)) for r in all_results)
        
            # Define products with their display names and corresponding result keys
            # Key names must match exactly what extract_all_et_products uses
            product_key_mapping = {
                'USGS BCM/Flint': 'bcm_aet',
                'USGS BCM/Reitz': 'reitz_et',  # Special case - uses reitz_et_mean_mm
                'USGS SSEBop-WB/Reitz': 'reitz_ssebop_wb',
                'USGS Reitz Ensemble': 'reitz_ensemble',
                'MOD16': 'mod16',
                'PMLv2': 'pmlv2',
                'SSEBop VIIRS': 'ssebop_viirs',
                'SSEBop MODIS': 'ssebop_modis',
                'OpenET Ensemble': 'openet',
                'OpenET SSEBop': 'openet_ssebop',
                'OpenET eeMETRIC': 'openet_eemetric',
                'OpenET DisALEXI': 'openet_disalexi',
                'OpenET geeSEBAL': 'openet_geesebal',
                'OpenET PT-JPL': 'openet_ptjpl',
                'WLDAS': 'wldas',
                'TerraClimate': 'terraclimate',
            }
        
            # Build products list based on Reitz availability
            if has_reitz:
                products = list(product_key_mapping.keys())
            else:
                products = [p for p in product_key_mapping.keys() if p != 'USGS BCM/Reitz']
        
            for product in products:
                ratio_data = {}
                product_key = product_key_mapping[product]
            
                for r in all_results:
                    if 'prism_ppt_mean_mm' in r and r['prism_ppt_mean_mm'] > 0:
                        et_mm = r.get(f'{product_key}_mean_mm', np.nan)
                    
                        if not np.isnan(et_mm):
                            ratio_data[r['basin_name']] = et_mm / r['prism_ppt_mean_mm']
            
                if ratio_data:
                    all_ratio_data[product] = ratio_data
                
                    # Create individual ratio map with correct year label
                    plot_et_ppt_ratio_map(
                        basins_gdf, ratio_data, product,
                        os.path.join(output_dir, f'et_ppt_ratio_{product.replace(" ", "_").replace("/", "_").lower()}.png'),
                        year_label=year_label
                    )
        
            # Create multi-panel ratio map with correct year label
            if all_ratio_data:
                plot_all_products_ratio_maps(basins_gdf, all_ratio_data, output_dir, year_label=year_label)

            # Store for combined maps
            basin_set_ratios[basin_set_key] = all_ratio_data

        # Store GDF for combined maps
        basin_set_gdfs[basin_set_key] = basins_gdf

        # Derive whole-IWV results by aggregating sub-basins (no extra GEE calls)
        if basin_set_key == 'subbasin':
            derive_whole_iwv_results(
                all_results, summary_df, basins_gdf, OUTPUT_DIR_WHOLE, year_label
            )

        print("\n" + "=" * 60)
        print("Processing complete!")
        print(f"Results saved to: {output_dir}")
        print("=" * 60)

    # Create combined calibration + sub-basin maps (if both sets were processed)
    if ('calibration' in basin_set_gdfs and 'subbasin' in basin_set_gdfs
            and 'calibration' in basin_set_ratios
            and 'subbasin' in basin_set_ratios):
        combined_dir = os.path.join(BASE_DIR, 'Outputs/IWV_BCM_ET_EVAL_combined/')
        print(f"\nCreating combined basin maps in {combined_dir}...")
        plot_all_combined_basin_ratio_maps(
            basin_set_gdfs['calibration'],
            basin_set_gdfs['subbasin'],
            basin_set_ratios['calibration'],
            basin_set_ratios['subbasin'],
            combined_dir,
            year_label=year_label
        )
        print(f"Combined maps saved to: {combined_dir}")


if __name__ == '__main__':
    main()
