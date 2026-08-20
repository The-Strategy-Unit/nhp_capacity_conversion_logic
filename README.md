# NHP Capacity Conversion

<!-- badges: start -->

[![codecov](https://codecov.io/gh/The-Strategy-Unit/nhp_capacity_conversion_logic/graph/badge.svg?token=D46wl0Y3vO)](https://codecov.io/gh/The-Strategy-Unit/nhp_capacity_conversion_logic)

[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

<!-- badges: end -->

This repository contains a Python CLI and Shiny application for converting NHP
demand-model activity aggregated into functional areas into capacity estimates.
It is a work in progress intended for internal use only.

## For developers

Prerequisites for running this model are on
[the team wiki](https://github.com/The-Strategy-Unit/nhp_products/wiki/How-to-run-capacity-conversion-model).

This package uses [`uv`](https://docs.astral.sh/uv/); the commands below were
verified with `uv 0.12.1`.

Run the complete capacity conversion pipeline for all sites or selected sites:

```console
uv run --locked -m nhp.capacity_conversion GUID
uv run --locked -m nhp.capacity_conversion GUID --ip-sites ALL --op-sites SITEA,SITEB --aae-sites SITEA
```

Run a single activity type:

```console
uv run --locked -m nhp.capacity_conversion.op GUID
uv run --locked -m nhp.capacity_conversion.aae GUID --sites SITEA
```

Running the pipeline will create a `results/GUID/RUNTIME` folder, with a
`capacity_conversion_results.xlsx` file within it.

## Shiny application

The application requires:

- `AZ_STORAGE_EP`: Azure Blob Storage account endpoint.
- `AZ_STORAGE_RESULTS`: container containing functional aggregations.
- `AZ_TABLE_ENDPOINT`: Azure Table Storage account endpoint.
- `TABLE_NAME`: table containing functional-aggregation metadata.

`FEEDBACK_FORM_URL` is optional. Set it to the `src` URL from the Microsoft
Forms
[embed code](https://support.microsoft.com/en-gb/office/share-a-form-384371be-f1e7-4628-bcba-abd3d6123917).
If it is unset, the feedback button reports that the form is unavailable.

Azure authentication uses `DefaultAzureCredential`. The credential must have
read access to both the Table catalogue and Blob results container. For local
Azure CLI authentication, run `az login` when needed.

The local application does not load `.env` automatically. Start the development
server from the repository root, pointing `uv` to your `.env` file:

```console
uv run --env-file .env --locked --group app shiny run --reload app.py
```

The application queries the table's `dev` partition. It presents permitted
datasets, `scenario_name` values and `scenario_runtime` model-run times, using
the selected entity's `RowKey` as the functional aggregation GUID. It loads all
four activity types and reshapes each aggregation across all sites before
capacity conversion.

On Posit Connect, `nhp_provider_<dataset>` grants access to one dataset, while
`nhp_devs` and `nhp_power_users` grant access to every available aggregation.
Unrecognised or absent Connect groups grant no dataset access. Local development
permits all available aggregations.

The Shiny dependencies are in the `app` dependency group. `requirements.txt` is
generated for Posit Connect and must not be edited manually.

Regenerate and validate the Connect requirements after changing dependencies:

```console
uv lock --check
uv export --no-default-groups --group app --no-hashes --output-file requirements.txt
uv pip compile requirements.txt --output-file /tmp/nhp-capacity-connect-requirements.txt
```

The final command reproduces Connect's dependency-resolution step and must
complete successfully.

## Deploying to Posit Connect

Consult the
[official Posit Connect publishing documentation](https://docs.posit.co/connect/user/publishing-cli/)
before using `rsconnect`.

The interactive deployment helper loads `.env` automatically and does not
override variables already set in the current environment. `.env` is ignored by
Git; never commit its credentials. In addition to the application runtime
variables above, set:

- `CONNECT_SERVER`: the Posit Connect server URL.
- `CONNECT_API_KEY`: a Posit Connect API key with permission to publish.
- `CONNECT_APP_ID`: the existing content GUID, required only for a redeployment.
  This is not the numeric content ID.

From the repository root, start the deployment interface with:

```console
uv run --locked --group dev scripts/deploy_shiny.py
```

Choose whether to create new content or replace an existing deployment. Before
deploying, the helper checks the required tools, bundle files, and environment
variables, then verifies the Connect server and asks for confirmation. It does
not display the API key or include it in subprocess arguments.

After the initial deployment, set its **Custom content URL** under **Settings →
Manage access** to:

```text
/nhp/dev/capacity-conversion/
```

The development application is available at
[connect.strategyunitwm.nhs.uk/nhp/dev/capacity-conversion/](https://connect.strategyunitwm.nhs.uk/nhp/dev/capacity-conversion/).
