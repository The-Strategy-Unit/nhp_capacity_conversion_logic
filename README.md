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

This package is built using [`uv`](https://docs.astral.sh/uv/). The commands below
were verified with `uv 0.12.1`. Run the capacity conversion pipeline using:

```console
uv run --locked -m nhp.capacity_conversion GUID # Run all settings
uv run --locked -m nhp.capacity_conversion.op GUID # Run Outpatient setting
uv run --locked -m nhp.capacity_conversion.aae GUID # Run Accident and Emergency setting
```.

Running the pipeline will create a `results/GUID/RUNTIME` folder, with a `capacity_conversion_results.xlsx` file within it.

## Shiny application

Run the application locally from the repository root:

```console
uv run --locked --group app shiny run --reload app.py
```

The application uses the project dependencies and the `app` dependency group in
`pyproject.toml`. `requirements.txt` is generated for Posit Connect and should
not be edited manually.

`nhp-products` declares an unversioned transitive Git source for `nhp-aci`.
The `[tool.uv.pip] no-sources = true` setting makes Connect's `uv pip` resolver
ignore that moving source and use the exact Git commits exported from `uv.lock`.
Regenerate and validate the Connect requirements after changing dependencies:

```console
uv lock --check
uv export --no-default-groups --group app --no-hashes --output-file requirements.txt
uv pip compile requirements.txt --output-file /tmp/nhp-capacity-connect-requirements.txt
```

The final command reproduces Connect's dependency-resolution step. It must
complete without a conflicting-URL error, and `requirements.txt` must contain
commit-pinned entries for both `nhp-products` and `nhp-aci`.

## Deploying to Posit Connect

Consult the [official Posit Connect publishing documentation](https://docs.posit.co/connect/user/publishing-cli/)
before using `rsconnect`.

The application requires these runtime environment variables:

- `AZ_STORAGE_EP`: Azure Storage account endpoint.
- `AZ_STORAGE_RESULTS`: results container name.
- `AZ_FUNC_AGG_OP_PATH`:
  `functional-aggregations/<version>/<guid>/op.parquet`.
- `AZ_FUNC_AGG_AAE_PATH`:
  `functional-aggregations/<version>/<guid>/aae.parquet`.
- `AZ_FUNC_AGG_IP_DAYCASE_PATH`:
  `functional-aggregations/<version>/<guid>/ip_daycase.parquet`.
- `AZ_FUNC_AGG_IP_MAT_PATH`:
  `functional-aggregations/<version>/<guid>/ip_maternity.parquet`.

All four paths are relative to the results container and must reference the
same model version and GUID.

Azure authentication uses `DefaultAzureCredential`; the Connect runtime must
provide a supported credential with read access to the results container.

The interactive deployment helper loads `.env` automatically and does not
override variables already set in the current environment. `.env` is ignored
by Git; never commit the API key. Set these variables before deploying:

- `CONNECT_SERVER`: the Posit Connect server URL.
- `CONNECT_API_KEY`: a Posit Connect API key with permission to publish.
- `CONNECT_APP_ID`: the existing content GUID, required only for a
  redeployment. This is not the numeric content ID.

From the repository root, start the deployment interface with:

```console
uv run --locked --group dev nhp-deploy
```

Choose whether to create new content or replace an existing deployment. Before
deploying, the helper checks the required tools, bundle files, and environment
variables, then verifies the Connect server and asks for confirmation. It does
not display the API key or include it in subprocess arguments.

After the initial deployment, set its **Custom content URL** under
**Settings → Manage access** to:

```text
/nhp/dev/capacity-conversion/
```

The development application is available at
[connect.strategyunitwm.nhs.uk/nhp/dev/capacity-conversion/](https://connect.strategyunitwm.nhs.uk/nhp/dev/capacity-conversion/).
