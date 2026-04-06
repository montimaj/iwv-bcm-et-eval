# Disclaimer

## Research and Scientific Use

This repository contains scripts, analyses, and derived outputs developed for
research purposes to evaluate actual evapotranspiration (AET) products over the
Indian Wells Valley (IWV), California, in the Great Basin. The work compares
multiple remote sensing and modeled ET products against the USGS Reitz ET
reference dataset for IWV Basin Characterization Model (BCM) calibration basins
and sub-basins over water years 2001-2015.

The code, figures, tables, and any derived results are provided **"AS IS"**,
without warranty of any kind, express or implied, including but not limited to
the warranties of merchantability, fitness for a particular purpose, accuracy,
completeness, or non-infringement. See the [LICENSE](LICENSE) file for the
full terms governing reuse of the software.

## No Endorsement

The views and conclusions expressed in this repository are those of the
author(s) and do not necessarily reflect the official policies or positions,
either expressed or implied, of the Desert Research Institute (DRI), the
U.S. Geological Survey (USGS), NASA, or any other organization whose data
products are ingested or referenced by this software. Mention of trade names,
commercial products, or third-party datasets does not constitute endorsement
or recommendation.

## Third-Party Data

This software ingests data from third-party providers, including but not
limited to USGS, NASA (MOD16), PML_V2, OpenET, SSEBop (VIIRS/MODIS), WLDAS,
TerraClimate, and PRISM. Users are responsible for complying with the terms
of use, citation requirements, and licensing of each upstream dataset.
Accuracy, availability, and versioning of these datasets are outside the
control of the author(s) of this repository.

## Limitations

Evapotranspiration estimates derived from remote sensing and land-surface
models are subject to substantial uncertainty arising from sensor limitations,
algorithmic assumptions, cloud contamination, spatial and temporal resolution
mismatches, scaling and aggregation effects, reference-dataset bias, and
basin-boundary delineation choices. Reported metrics (MBD, RMSD, MAD,
correlation coefficients, CV, etc.) characterize relative agreement among
products and should not be interpreted as absolute measures of ET accuracy.
Product rankings may change with alternative reference datasets, time
periods, spatial domains, or aggregation methods.

## Use in Legal, Regulatory, or Adjudicative Proceedings

**This software and its outputs were not designed, calibrated, or validated
for use as evidence in litigation, water-rights adjudication, regulatory
compliance, or any legal or administrative proceeding.** Any such use is
undertaken solely at the user's own risk and responsibility.

If outputs from this repository are considered for use in a legal,
regulatory, or adjudicative context (including but not limited to water
court proceedings), the user must:

1. Engage qualified hydrologic, remote-sensing, and statistical experts to
   independently review the methodology, input data, assumptions, code, and
   results for fitness to the specific question at issue;
2. Independently verify all inputs, intermediate products, and outputs
   against authoritative primary sources;
3. Characterize and disclose all relevant uncertainties, limitations, and
   assumptions; and
4. Not represent the outputs as authoritative, final, or endorsed by the
   author(s), the Desert Research Institute, or any data provider.

The author(s) and their affiliated institutions make no representation that
the software or its outputs meet any evidentiary, regulatory, or legal
standard (including, without limitation, Daubert, Frye, or Federal Rule of
Evidence 702 standards in the United States). The author(s) and their
affiliated institutions shall not be liable for any claim, damages, or other
liability arising from the use, misuse, citation, or reliance on this
software or its outputs in any legal, regulatory, or adjudicative context.
