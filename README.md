# NHP Capacity Conversion

<!-- badges: start -->
[![codecov](https://codecov.io/gh/The-Strategy-Unit/nhp_capacity_conversion_logic/graph/badge.svg?token=D46wl0Y3vO)](https://codecov.io/gh/The-Strategy-Unit/nhp_capacity_conversion_logic)

[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
<!-- badges: end -->

This monorepo contains the logic and Shiny application for converting NHP
Demand model results, which have been aggregated into functional areas, into
capacity requirements.

This is currently a work in progress and intended for internal use only.

## For developers

This section is aimed at maintainers of the package who work for The Strategy Unit Data Science team.

Prerequisites for running this model are on [the team wiki](https://github.com/The-Strategy-Unit/nhp_products/wiki/How-to-run-capacity-conversion-model).

This package is built using [`uv`](https://docs.astral.sh/uv/). If you have `uv` installed, run the capacity conversion pipeline using:   

```console
uv run -m nhp.capacity_conversion GUID # Run all settings
uv run -m nhp.capacity_conversion.op GUID # Run Outpatient setting
uv run -m nhp.capacity_conversion.aae GUID # Run Accident and Emergency setting 
```.

Running the pipeline will create a `results/GUID/RUNTIME` folder, with a `capacity_conversion_results.xlsx` file within it.

## Shiny application

Run the application locally from the repository root:

```console
uv run --group app shiny run --reload app.py
```

The application dependencies are managed in the `app` dependency group in
`pyproject.toml`. `requirements.txt` is generated for Posit Connect and should
not be edited manually. Regenerate it after changing application dependencies:

```console
uv export --only-group app --no-emit-project --no-hashes --output-file requirements.txt
```

## Deploying to Posit Connect

Consult the [official Posit Connect publishing documentation](https://docs.posit.co/connect/user/publishing-cli/)
before using `rsconnect`.

The deployment commands require `CONNECT_SERVER` and `CONNECT_API_KEY` to be
exported in the current shell. Do not store the API key in this repository.
Confirm connectivity before deploying:

```console
uv run --group app rsconnect details \
    -s "$CONNECT_SERVER" \
    -k "$CONNECT_API_KEY"
```

For the initial deployment only, create new content with:

```console
uv run --group app rsconnect deploy shiny \
    -s "$CONNECT_SERVER" \
    -k "$CONNECT_API_KEY" \
    --new \
    --title "NHP Capacity Conversion (development)" \
    --entrypoint app:app \
    --requirements-file requirements.txt \
    --package-installer UV \
    --exclude "**" \
    . \
    app.py \
    _brand.yml \
    requirements.txt
```

Do not use `--new` for subsequent deployments. Update the existing development
content using its Connect app ID. Set `CONNECT_APP_ID` to that ID in the
current shell; this is a deployment identifier, not a secret:

```console
uv run --group app rsconnect deploy shiny \
    -s "$CONNECT_SERVER" \
    -k "$CONNECT_API_KEY" \
    --app-id "$CONNECT_APP_ID" \
    --title "NHP Capacity Conversion (development)" \
    --entrypoint app:app \
    --requirements-file requirements.txt \
    --package-installer UV \
    --exclude "**" \
    . \
    app.py \
    _brand.yml \
    requirements.txt
```

After the initial deployment, set its **Custom content URL** under
**Settings → Manage access** to:

```text
/nhp/dev/capacity-conversion/
```

The development application is available at
[connect.strategyunitwm.nhs.uk/nhp/dev/capacity-conversion/](https://connect.strategyunitwm.nhs.uk/nhp/dev/capacity-conversion/).

The development deployment was verified on 4 August 2026 using Posit Connect
2026.03.1 and Python 3.12.11. It displays:

> NHP Capacity Conversion is running.

