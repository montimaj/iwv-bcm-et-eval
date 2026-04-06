# iwv-bcm-et-eval

Actual evapotranspiration (AET) assessment over the Indian Wells Valley
(IWV), California, in the Great Basin. This repository compares multiple
remote-sensing and land-surface-model AET products against the USGS Reitz ET
and BCM AET (Flint et al., 2021) reference datasets for the IWV Basin
Characterization Model (BCM) calibration basins and IWV sub-basins, over
water years 2001-2015.

## Project Structure

```
iwv-bcm-et-eval/
├── CITATION.cff                            # Citation metadata
├── DISCLAIMER.md                           # Legal disclaimer
├── LICENSE                                 # MIT License
├── README.md
├── Data/
│   ├── USGS/
│   │   ├── BCM_AET/aet_WYs_1896_2024/     # USGS BCM/Flint 270 m rasters (external)
│   │   ├── IWV_calibrationBasin/           # Calibration basin shapefiles (external)
│   │   ├── IWV_SubBasin/                   # Sub-basin shapefiles (external)
│   │   └── ReitzET_GB_wy_averages_and_POR_ave_mm/  # 200 m Reitz ET rasters (external)
│   └── Outputs/
│       ├── IWV_BCM_ET_EVAL_calibration/    # Output: calibration basins (generated)
│       ├── IWV_BCM_ET_EVAL_subbasin/       # Output: sub-basins (generated)
│       ├── IWV_BCM_ET_EVAL_whole/          # Output: whole IWV basin (generated)
│       └── IWV_BCM_ET_EVAL_combined/       # Output: combined basin maps (generated)
├── scripts/
│   ├── bcm_et_eval_iwv.py                 # Main ET evaluation script
│   └── et_comparison_viewer_iwv.js        # GEE split-panel viewer app
```

## Interactive GEE App

A companion Google Earth Engine split-panel viewer allows side-by-side
comparison of any two ET products with monthly and annual time-series
charts:

**[IWV ET Explorer](https://nwi-usgs.projects.earthengine.app/view/iwv-et-explorer)**

Features:
- Split-panel map with any two ET products (mean annual or single-year)
- **Pixel mode**: click anywhere to see monthly and annual ET at that point
- **Basin mode**: click within a basin to see spatially averaged ET over
  the entire sub-basin (HU-10) or calibration basin (HU-12)
- Toggleable basin overlays (IWV sub-basins in black, calibration basins
  in red)
- Imperial (inches/ft) and metric (mm) chart units
- Adjustable colorbar range and year selection

Source: [scripts/et_comparison_viewer_iwv.js](scripts/et_comparison_viewer_iwv.js)

## ET Products Compared

- USGS BCM/Flint (local 270 m ASC rasters, `Data/USGS/BCM_AET/aet_WYs_1896_2024`)
- USGS Reitz ET (local 200 m ADF rasters, `Data/USGS/ReitzET_GB_wy_averages_and_POR_ave_mm`)
- USGS Reitz Ensemble (`projects/nwi-usgs/assets/USGS-Reitz-Ensemble-ET`)
- USGS Reitz SSEBop-WB (`projects/nwi-usgs/assets/USGS-Reitz-SSEBop-WB`)
- MOD16 (`MODIS/061/MOD16A2GF`)
- PML_V2 (`projects/pml_evapotranspiration/PML/OUTPUT/PML_V22a`)
- SSEBop VIIRS (`projects/usgs-ssebop/viirs_et_v6_monthly`)
- SSEBop MODIS (`projects/usgs-ssebop/modis_et_v5_monthly`)
- OpenET Ensemble / SSEBop / eeMETRIC / DisALEXI / geeSEBAL / PT-JPL / SIMS
  (`OpenET/.../CONUS/GRIDMET/MONTHLY/v2_0`)
- WLDAS ET (`projects/climate-engine-pro/assets/ce-wldas/daily`)
- TerraClimate (`IDAHO_EPSCOR/TERRACLIMATE`)

## Analyses

1. Basin-level spatial averages of mean annual AET (ac-ft/yr) as bar charts
2. Maps of mean annual AET / mean annual PRISM PPT ratios
3. Annual ET time-series comparison plots for each basin
4. Difference metrics (MBD, RMSD, MAD) vs USGS Reitz reference
5. Correlation analysis (Pearson, Spearman) for each product
6. Inter-product agreement (CV, range) across basins

![Mean Annual AET/PPT Ratio by ET Product (WY2001-2015)](Data/Outputs/IWV_BCM_ET_EVAL_combined/all_products_combined_et_ppt_ratio_maps.png)

**Figure 1.** Mean annual AET/PPT ratio across IWV sub-basins (dark gray
outlines) and BCM calibration basins (dark magenta outlines) for 16 ET
products (WY2001-2015). Ratios below 1.0 (blue) indicate AET < PPT
(potential recharge); ratios above 1.0 (red) indicate AET > PPT. The USGS
BCM/Flint and BCM/Reitz products consistently show low ratios on the desert
valley floor, supporting the BCM recharge signal identified in
[Saleh et al. (2026)](https://doi.org/10.3133/sir20265114). ET products
show better agreement across most calibration basins (mountain watersheds),
with the notable exception of the Playa basin where estimates diverge
substantially. Several remote-sensing products (e.g., OpenET SSEBop, OpenET
PT-JPL) estimate AET exceeding PPT in portions of the valley floor and
Playa.

## Usage

```bash
# Default: process both basin sets, GEE + local Reitz ET
python scripts/bcm_et_eval_iwv.py

# Only one basin set
python scripts/bcm_et_eval_iwv.py --basin-set calibration
python scripts/bcm_et_eval_iwv.py --basin-set subbasin

# Test mode (2 basins)
python scripts/bcm_et_eval_iwv.py --test
```

Outputs are written to `Data/Outputs/IWV_BCM_ET_EVAL_calibration/`,
`Data/Outputs/IWV_BCM_ET_EVAL_subbasin/`, and
`Data/Outputs/IWV_BCM_ET_EVAL_whole/`.

## Installation

### 1. Create a conda environment

```bash
conda create -n iwv-bcm-et-eval python=3.12 -y
conda activate iwv-bcm-et-eval
```

### 2. Install dependencies

```bash
conda install -c conda-forge numpy pandas geopandas matplotlib rasterio \
  shapely earthengine-api tqdm dask -y
```

### 3. Google Earth Engine Authentication

This project relies on the Google Earth Engine (GEE) Python API for
downloading (and reducing) some of the ET datasets from the GEE data
repository. After completing step 2, run:

```bash
earthengine authenticate
```

The installation and authentication guide for the Earth Engine Python API
is available [here](https://developers.google.com/earth-engine/guides/python_install).
The [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) tools may
be required for this GEE authentication step. You also need to create a
Google Cloud project to use the GEE API.

To verify authentication:

```bash
python -c "import ee; ee.Initialize(); print('GEE authenticated successfully')"
```

> **Note:** Some assets used by this project (e.g.,
> `projects/nwi-usgs/assets/...`, `projects/usgs-ssebop/...`,
> `projects/climate-engine-pro/...`) require explicit access permissions.
> Contact the asset owners if you receive permission-denied errors.

### 4. Local data

The following external datasets are required and must be placed in
`Data/USGS/` before running the script:

- **Basin shapefiles** — IWV calibration basin and sub-basin boundaries
  (`Data/USGS/IWV_calibrationBasin/`, `Data/USGS/IWV_SubBasin/`), available
  from [Saleh & Flint (2019)](https://doi.org/10.5066/F7JM28XZ).
- **USGS BCM/Flint rasters** (`Data/USGS/BCM_AET/aet_WYs_1896_2024/`) — 270 m
  annual AET from the Basin Characterization Model, available from
  [Flint et al. (2021b)](https://doi.org/10.5066/P9PT36UI).
- **200 m resampled Reitz ET rasters**
  (`Data/USGS/ReitzET_GB_wy_averages_and_POR_ave_mm/`). This product is not
  publicly distributed; however, it is similar to the publicly available
  SSEBop-WB product
  ([Reitz et al., 2017b](https://doi.org/10.3390/rs9121181)).

Contact the authors of
[Saleh et al. (2026)](https://doi.org/10.3133/sir20265114) to obtain the resampled Reitz ET dataset.

## Author

Dr. Sayantan Majumdar (Desert Research Institute) — sayantan.majumdar@dri.edu

## License

Released under the MIT License. See [LICENSE](LICENSE) for the full terms.

## Disclaimer

This software is provided for research purposes only and is **not intended
or validated for use in legal, regulatory, or adjudicative proceedings**
(including water court). Please read [DISCLAIMER.md](DISCLAIMER.md) in full
before using this software or its outputs in any such context.

## AI Disclosure

Portions of this codebase and documentation were developed with the
assistance of AI tools (Claude, Anthropic). All AI-generated content was
reviewed, validated, and approved by the author. The scientific analyses,
interpretations, and conclusions are solely the responsibility of the
author.

## Citation

If you use this software or its outputs, please cite it using the metadata
in [CITATION.cff](CITATION.cff).

## Data Sources and Citations

Users of this software must also cite the upstream datasets and the IWV
BCM case study that motivates the analysis. Reference links for each ET
product are also documented in the header of
[scripts/et_comparison_viewer_iwv.js](scripts/et_comparison_viewer_iwv.js).

### IWV Basin Characterization Model (BCM)

- [Flint et al. (2021a)](https://doi.org/10.3133/tm6H1) ·
  [Flint et al. (2021b)](https://doi.org/10.5066/P9PT36UI) ·
  [Saleh et al. (2026)](https://doi.org/10.3133/sir20265114)

### ET Products

| Product | Reference(s) |
|---|---|
| USGS BCM/Flint | [Flint et al. (2021a)](https://doi.org/10.3133/tm6H1) · [Flint et al. (2021b)](https://doi.org/10.5066/P9PT36UI) |
| USGS Reitz Ensemble ET | <https://doi.org/10.1029/2022WR034012> · <https://doi.org/10.5066/P9EZ3VAS> |
| USGS Reitz SSEBop-WB | <https://doi.org/10.3390/rs9121181> · <https://doi.org/10.5066/F7QC02FK> |

> **Note on the Reitz ET product used in the IWV BCM:**
> [Saleh et al. (2026)](https://doi.org/10.3133/sir20265114) cites
> [Reitz et al. (2017a)](https://doi.org/10.1111/1752-1688.12546), but
> the ET product actually used is from
> [Reitz et al. (2017b)](https://doi.org/10.3390/rs9121181). This is
> evident from the methodology described on page 12 of the USGS report
> ([Saleh et al., 2026](https://doi.org/10.3133/sir20265114)).

| MOD16 (`MODIS/061/MOD16A2GF`) | [Running et al. (2019)](https://modis-land.gsfc.nasa.gov/pdf/MOD16UsersGuideV2.022019.pdf) · [GEE Catalog](https://developers.google.com/earth-engine/datasets/catalog/MODIS_061_MOD16A2GF#description) |
| PML_V2 | [Zhang et al. (2019)](https://doi.org/10.1016/j.rse.2018.12.031) · [Xu et al. (2026)](https://doi.org/10.5194/essd-2026-94) · [GEE Catalog](https://developers.google.com/earth-engine/datasets/catalog/projects_pml_evapotranspiration_PML_OUTPUT_PML_V22a#description) |
| SSEBop VIIRS | [Senay et al. (2013)](https://doi.org/10.1111/jawr.12057) · [Senay (2018)](https://elibrary.asabe.org/abstract.asp?AID=48975&t=3&dabs=Y&redir=&redirType=) · [Senay et al. (2020)](https://www.mdpi.com/1424-8220/20/7/1915) · [Senay et al. (2022)](https://doi.org/10.1016/j.rse.2022.113011) · [Senay et al. (2023)](https://doi.org/10.3390/rs15010260) · [Chiew et al. (2002)](#references) · [GEE Community Catalog](https://gee-community-catalog.org/projects/usgs_viirs/) |
| SSEBop MODIS | [Senay et al. (2020)](https://doi.org/10.5066/P9OUVUUI) · [Senay et al. (2011)](https://doi.org/10.1016/j.agwat.2010.10.014) · [Senay et al. (2007)](https://doi.org/10.3390/s7060979) · [Velpuri et al. (2013)](https://doi.org/10.1016/j.rse.2013.07.013) · [GEE Community Catalog](https://gee-community-catalog.org/projects/usgs_modis_et/) |
| OpenET Ensemble | [Melton et al. (2022)](https://doi.org/10.1111/1752-1688.12956) · [Volk et al. (2024)](https://doi.org/10.1038/s44221-023-00181-7) · [GEE Catalog](https://developers.google.com/earth-engine/datasets/catalog/OpenET_ENSEMBLE_CONUS_GRIDMET_MONTHLY_v2_0) |
| OpenET SSEBop | <https://developers.google.com/earth-engine/datasets/catalog/OpenET_SSEBOP_CONUS_GRIDMET_MONTHLY_v2_0> |
| OpenET eeMETRIC | <https://developers.google.com/earth-engine/datasets/catalog/OpenET_EEMETRIC_CONUS_GRIDMET_MONTHLY_v2_0> |
| OpenET DisALEXI | <https://developers.google.com/earth-engine/datasets/catalog/OpenET_DISALEXI_CONUS_GRIDMET_MONTHLY_v2_0> |
| OpenET geeSEBAL | <https://developers.google.com/earth-engine/datasets/catalog/OpenET_GEESEBAL_CONUS_GRIDMET_MONTHLY_v2_0> |
| OpenET PT-JPL | <https://developers.google.com/earth-engine/datasets/catalog/OpenET_PTJPL_CONUS_GRIDMET_MONTHLY_v2_0> |
| OpenET SIMS | <https://developers.google.com/earth-engine/datasets/catalog/OpenET_SIMS_CONUS_GRIDMET_MONTHLY_v2_0> |
| WLDAS ET | [Erlingis et al. (2021)](https://doi.org/10.1111/1752-1688.12910) · [Climate Engine](https://support.climateengine.org/article/137-wldas) |
| TerraClimate | [Abatzoglou et al. (2018)](https://doi.org/10.1038/sdata.2017.191) · [GEE Catalog](https://developers.google.com/earth-engine/datasets/catalog/IDAHO_EPSCOR_TERRACLIMATE#description) |

## References

- Abatzoglou, J.T., Dobrowski, S.Z., Parks, S.A., & Hegewisch, K.C.
  (2018). TerraClimate, a high-resolution global dataset of monthly climate
  and climatic water balance from 1958–2015. *Scientific Data*, 5(1),
  170191. <https://doi.org/10.1038/sdata.2017.191>

- Chiew, F., Wang, Q.J., McConachy, F., James, R., Wright, W., & deHoedt,
  G. (2002). Evapotranspiration maps for Australia. *Hydrology and Water Resources Symposium*, Melbourne, 20–23, 2002, Institution of Engineers,
  Australia.

- Erlingis, J.M., Rodell, M., Peters-Lidard, C.D., Li, B., Kumar, S.V.,
  Famiglietti, J.S., Granger, S.L., Hurley, J.V., Liu, P., & Mocko, D.M.
  (2021). A High-Resolution Land Data Assimilation System Optimized for the
  Western United States. *JAWRA Journal of the American Water Resources Association*, 57(5), 692–710. <https://doi.org/10.1111/1752-1688.12910>

- Flint, L.E., Flint, A.L., & Stern, M.A. (2021). The basin characterization model—A regional water balance software package. *U.S. Geological Survey Techniques and Methods 6–H1*, 85 p. https://doi.org/10.3133/tm6H1.

- Flint, L.E., Flint, A.L., Stern, M.A., & Seymour, W.A. (2021). The Basin Characterization Model - A monthly regional water balance software package (BCMv8) data release and model archive for hydrologic California (ver. 5.0, June 2025). *U.S. Geological Survey data release*. https://doi.org/10.5066/P9PT36UI.

- Melton, F., Huntington, J., Grimm, R., Herring, J., Hall, M., Rollison,
  D., Erickson, T., Allen, R., Anderson, M., Fisher, J.B., Kilic, A.,
  Senay, G.B., Volk, J., Hain, C., Johnson, L., Ruhoff, A., Blankenau, P.,
  Bromley, M., Carrara, W., ... & Anderson, R.G. (2022). OpenET: Filling a
  Critical Data Gap in Water Management for the Western United States.
  *JAWRA Journal of the American Water Resources Association*.
  <https://doi.org/10.1111/1752-1688.12956>

- Reitz, M., Sanford, W.E., Senay, G.B., & Cazenas, J. (2017a). Annual
  Estimates of Recharge, Quick-Flow Runoff, and Evapotranspiration for the
  Contiguous U.S. Using Empirical Regression Equations. *JAWRA Journal of the American Water Resources Association*, 53(4), 961–983.
  <https://doi.org/10.1111/1752-1688.12546>

- Reitz, M., Senay, G., & Sanford, W. (2017b). Combining Remote Sensing
  and Water-Balance Evapotranspiration Estimates for the Conterminous
  United States. *Remote Sensing*, 9(12), 1181.
  <https://doi.org/10.3390/rs9121181>

- Running, S.W., Mu, Q., Zhao, M., & Moreno, A. (2019). MODIS Global
  Terrestrial Evapotranspiration (ET) Product (MOD16A2/A3 and Year-end
  Gap-filled MOD16A2GF/A3GF) NASA Earth Observing System MODIS Land
  Algorithm (For Collection 6).
  <https://modis-land.gsfc.nasa.gov/pdf/MOD16UsersGuideV2.022019.pdf>

- Saleh, D., & Flint, L.E. (2019). Indian Wells Valley, California, sub-watersheds for the Basin Characterization Model. *U.S. Geological Survey data release*. https://doi.org/10.5066/F7JM28XZ.

- Saleh, D., Flint, L., & Stern, M. (2026). *Assessing natural recharge in
  Indian Wells Valley, California—A Basin Characterization Model case
  study* (ver. 1.1, March 2026). *U.S. Geological Survey Scientific Investigations Report 2026–5114*, 34 p.
  <https://doi.org/10.3133/sir20265114>

- Senay, G.B. (2018). Satellite psychrometric formulation of the
  Operational Simplified Surface Energy Balance (SSEBop) model for
  quantifying and mapping evapotranspiration. *Applied Engineering in Agriculture*, 34(3), 555–566.
  <https://elibrary.asabe.org/abstract.asp?AID=48975&t=3&dabs=Y&redir=&redirType=>

- Senay, G.B., Bohms, S., Singh, R.K., Gowda, P.H., Velpuri, N.M., Alemu,
  H., & Verdin, J.P. (2013). Operational evapotranspiration mapping using
  remote sensing and weather datasets: A new parameterization for the SSEB
  approach. *JAWRA Journal of the American Water Resources Association*,
  49(3), 577–591. <https://doi.org/10.1111/jawr.12057>

- Senay, G.B., Budde, M., Verdin, J.P., & Melesse, A.M. (2007). A coupled
  remote sensing and simplified surface energy balance approach to estimate
  actual evapotranspiration from irrigated fields. *Sensors*, 7(6),
  979–1000. <https://doi.org/10.3390/s7060979>

- Senay, G.B., Budde, M.E., & Verdin, J.P. (2011). Enhancing the
  Simplified Surface Energy Balance (SSEB) approach for estimating
  landscape ET: Validation with the METRIC model. *Agricultural Water Management*, 98(4), 606–618.
  <https://doi.org/10.1016/j.agwat.2010.10.014>

- Senay, G.B., Friedrichs, M., Morton, C., Parrish, G.E., Schauer, M.,
  Khand, K., Kagone, S., Boiko, O., & Huntington, J. (2022). Mapping
  actual evapotranspiration using Landsat for the conterminous United
  States: Google Earth Engine implementation and assessment of the SSEBop
  model. *Remote Sensing of Environment*, 275, 113011.
  <https://doi.org/10.1016/j.rse.2022.113011>

- Senay, G.B., Kagone, S., & Velpuri, N.M. (2020). Operational Global
  Actual Evapotranspiration: Development, Evaluation and Dissemination.
  *Sensors*, 20(7), 1915. <https://www.mdpi.com/1424-8220/20/7/1915>.
  Data release: <https://doi.org/10.5066/P9OUVUUI>

- Senay, G.B., Parrish, G.E., Schauer, M., Friedrichs, M., Khand, K.,
  Boiko, O., Kagone, S., Dittmeier, R., Arab, S., & Ji, L. (2023).
  Improving the Operational Simplified Surface Energy Balance
  Evapotranspiration Model Using the Forcing and Normalizing Operation.
  *Remote Sensing*, 15(1), 260. <https://doi.org/10.3390/rs15010260>

- Velpuri, N.M., Senay, G.B., Singh, R.K., Bohms, S., & Verdin, J.P.
  (2013). A comprehensive evaluation of two MODIS evapotranspiration
  products over the conterminous United States: Using point and gridded
  FLUXNET and water balance ET. *Remote Sensing of Environment*, 139,
  35–49. <https://doi.org/10.1016/j.rse.2013.07.013>

- Volk, J.M., Huntington, J.L., Melton, F.S., Allen, R., Anderson, M.,
  Fisher, J.B., Kilic, A., Ruhoff, A., Senay, G.B., Minor, B., Morton, C.,
  Ott, T., Johnson, L., de Andrade, B., Carrara, W., Doherty, C.T.,
  Dunkerly, C., Friedrichs, M., Guzman, A., ... & Yang, Y. (2024).
  Assessing the accuracy of OpenET satellite-based evapotranspiration data
  to support water resource and land management applications. *Nature Water*, 2(2), 193–205. <https://doi.org/10.1038/s44221-023-00181-7>

- Xu, Z., Zhang, Y., Kong, D., Ma, N., & Zhang, X. (2026). Extended global
  terrestrial evapotranspiration and gross primary production dataset from
  1982 to near present. *EGU Earth System Science Data Discussions*. <https://doi.org/10.5194/essd-2026-94>

- Zhang, Y., Kong, D., Gan, R., Chiew, F.H.S., McVicar, T.R., Zhang, Q.,
  & Yang, Y. (2019). Coupled estimation of 500 m and 8-day resolution
  global evapotranspiration and gross primary production in 2002–2017.
  *Remote Sensing of Environment*, 222, 165–182.
  <https://doi.org/10.1016/j.rse.2018.12.031>
