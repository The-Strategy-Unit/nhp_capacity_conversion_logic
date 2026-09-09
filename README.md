# OpenPlan Capacity Conversion Model

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
- `CAPACITY_MODEL_VERSION`: functional-aggregation catalogue partition and blob
  path version, such as `dev` or `prod`.
- `TABLE_NAME`: table containing functional-aggregation metadata.

`FEEDBACK_FORM_URL` is required. Set it to the `src` URL from the Microsoft Forms
[embed code](https://support.microsoft.com/en-gb/office/share-a-form-384371be-f1e7-4628-bcba-abd3d6123917).
Deployment rejects a missing or invalid URL. The application still reports that
the form is unavailable if the runtime configuration is unexpectedly missing.

Azure authentication uses `DefaultAzureCredential`. The credential must have
read access to both the Table catalogue and Blob results container. For local
Azure CLI authentication, run `az login` when needed.

The local application does not load `.env` automatically. Start the development
server from the repository root, pointing `uv` to your `.env` file:

```console
uv run --env-file .env --locked --group app shiny run --reload app.py
```

The application queries the table partition configured by
`CAPACITY_MODEL_VERSION`. It presents permitted datasets, `scenario_name`
values and `scenario_runtime` model-run times, using the selected entity's
`RowKey` as the functional aggregation GUID. It loads OP, A&E, IP day-case, IP
maternity and IP wards aggregations, reshaping each across all sites before
capacity conversion. It displays their capacity summaries and includes all
five activity types in the Excel download.

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

The interactive deployment helper loads `.env` automatically. Values in `.env`
override variables already set in the current environment, making `.env` the
source of truth for deployment. `.env` is ignored by Git; never commit its
credentials. In addition to the application runtime variables above, set:

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
variables, reports whether each effective value came from `.env` or the current
environment, and rejects invalid HTTPS endpoints. It then verifies the Connect
server and asks for confirmation. It does not display environment variable
values or include the API key in subprocess arguments.

### Automated deployment

The [Deploy Shiny application workflow](.github/workflows/deploy-shiny.yaml)
redeploys the existing application to:

- the `dev` Posit Connect environment after a push to `main`; and
- the `prod` Posit Connect environment when a GitHub release is published.

Configure GitHub Environments named `dev` and `prod`. Define all of the
following as environment secrets in each one so the same workflow can select
the target's configuration and GitHub can mask their values in workflow logs:

- `CONNECT_SERVER`
- `CONNECT_APP_ID` (the existing content GUID, not the numeric content ID)
- `CONNECT_API_KEY`
- `AZ_STORAGE_EP`
- `AZ_STORAGE_RESULTS`
- `AZ_TABLE_ENDPOINT`
- `CAPACITY_MODEL_VERSION`
- `TABLE_NAME`
- `FEEDBACK_FORM_URL`

The API key must be able to publish the content identified by that environment's
`CONNECT_APP_ID`. The workflow passes the runtime secrets to Connect but does
not pass the API key to the deployed application. It ignores `.env` during
automated deployment so repository content cannot override GitHub secrets.
Configure required reviewers and deployment protection rules on the `prod`
GitHub Environment.

Pull requests and deployments use the same reusable CI workflow for the
lockfile, generated requirements, formatting, lint, type, unit and browser
checks. Requiring its pull-request status check is optional; deployment always
runs the complete CI workflow again against the exact commit being deployed.
Deployments are serialized per environment, with up to 100 runs queued. GitHub
does not guarantee queue order, so production accepts only the highest stable
`vMajor.Minor.Patch` tag on `main`. A queued dev run stops before expensive CI
when a newer commit reaches `main`, then checks the tip again immediately before
deployment.

The deployment starts only after verification succeeds. It checks that
`CONNECT_APP_ID` resolves to a Python Shiny application with the expected
environment-specific title and a content URL on `CONNECT_SERVER` before
replacing it. After deployment it permits only same-origin HTTPS redirects and
requires HTTP 200, retrying transient failures three times.

This workflow requires Posit Connect 2025.06 or later. With rsconnect-python
1.30, those releases verify a draft bundle before activating it, leaving the
previous bundle active when built-in verification fails.

Posit Connect applies supplied runtime environment variables before uploading
the new bundle. Consequently, a failed deployment can leave the previous bundle
running with newly supplied configuration. Test configuration changes in dev
first and retain the previous production values securely until the production
deployment succeeds; the workflow cannot make this Connect operation atomic.

Both Connect applications must be created manually before the workflow's first
run. The local helper remains the supported way to create that initial content.

#### Production rollback

The version guard deliberately prevents redeploying an older GitHub release. If
the latest production deployment is unhealthy, activate the previous known-good
bundle from the application's **Content Bundles** page in Connect. Restore any
previous runtime configuration separately because it is not stored in the
bundle. Then revert the faulty change on `main` and publish a new patch release;
for example, recover from `v1.4.2` with `v1.4.3` rather than rerunning `v1.4.1`.

After the initial deployment, set its **Custom content URL** under **Settings →
Manage access** to:

```text
/nhp/dev/capacity-conversion/
```

The development application is available at
[connect.strategyunitwm.nhs.uk/nhp/dev/capacity-conversion/](https://connect.strategyunitwm.nhs.uk/nhp/dev/capacity-conversion/).
