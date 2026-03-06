# NHP Capacity Conversion Logic

<!-- badges: start -->
[![codecov](https://codecov.io/gh/The-Strategy-Unit/nhp_capacity_conversion_logic/graph/badge.svg?token=D46wl0Y3vO)](https://codecov.io/gh/The-Strategy-Unit/nhp_capacity_conversion_logic)

[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
<!-- badges: end -->

This repository contains the logic for converting NHP Demand model results, which have been aggregated into functional areas, into capacity requirements.

This is currently a work in progress and intended for internal use only.

## For developers

This section is aimed at maintainers of the package who work for The Strategy Unit Data Science team.

Prerequisites for running this model are on [the team wiki](https://github.com/The-Strategy-Unit/nhp_products/wiki/How-to-run-capacity-conversion-model).

This package is built using [`uv`](https://docs.astral.sh/uv/). If you have `uv` installed, run the capacity conversion pipeline using: `uv run -m nhp.capacity_conversion.aae GUID`.

Running the pipeline will create a `results/GUID/RUNTIME` folder, with a `capacity_conversion_results.xlsx` file within it.
