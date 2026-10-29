# Manual setup

Everything below is done once, by hand, in the Hugging Face and GitHub web interfaces. Nothing
here needs a credit card. Replace `<owner>` with your Hugging Face user or organization name.

## 1. Hugging Face

1. Create two **dataset** repositories (New, Dataset):
   - `<owner>/cvm-fundos-lake-dev`, visibility **private**
   - `<owner>/cvm-fundos-lake`, visibility **public**

   The names are fixed by `config/pipeline.yml`. The pipeline refuses to run if the dev repo
   is public or the prod repo is private.
2. Create two **fine grained** access tokens (Settings, Access Tokens, Create new token,
   Fine-grained):
   - `cvm-fundwatch-dev`: under "Repositories permissions" select only
     `<owner>/cvm-fundos-lake-dev` and tick read and write access to its contents.
   - `cvm-fundwatch-prod`: same, only `<owner>/cvm-fundos-lake`.

   Leave every other permission unticked. Copy each token once; HF shows it only at creation.
3. Optional: check your private storage quota in the HF billing/usage page before a large dev
   backfill. The prod repo is public and the dev repo only needs a few months.

4. Create two **model** repositories (New, Model) for the Isolation Forest:
   - `<owner>/cvm-fundos-anomaly-dev`, **private**
   - `<owner>/cvm-fundos-anomaly`, **public**
5. Create two more fine grained tokens, used by training and read by the ETL:
   - `cvm-fundwatch-model-dev`: write access to `<owner>/cvm-fundos-anomaly-dev`, read access
     to `<owner>/cvm-fundos-lake-dev`.
   - `cvm-fundwatch-model-prod`: write access to `<owner>/cvm-fundos-anomaly`, read access to
     `<owner>/cvm-fundos-lake`.

## 2. GitHub repository

1. Create the public repository `cvm-fundwatch` and push `main`.
2. Create the branch `development` from `main` and push it.
3. Settings, Actions, General:
   - Workflow permissions: **Read repository contents and packages permissions**.
   - Leave "Allow GitHub Actions to create and approve pull requests" unticked.
4. Settings, Secrets and variables, Actions, **Variables** tab, repository variable:
   - `HF_OWNER` = `<owner>`

## 3. GitHub Environments

Settings, Environments, New environment, three times:

| Environment | Deployment branches | Required reviewers | Secret | Variable |
|---|---|---|---|---|
| `dev` | Selected branches: `main`, `development` | none | `HF_TOKEN` = dev token, `HF_MODEL_TOKEN` = dev model token | `CVM_LAKE_ENV` = `dev` |
| `prod` | Selected branches: `main` | none | `HF_TOKEN` = prod token, `HF_MODEL_TOKEN` = prod model token | `CVM_LAKE_ENV` = `prod` |
| `prod-approval` | Selected branches: `main` | you | none | none |

Why `dev` allows `main`: the schedule always runs from the default branch. The dev job then
checks out `development`, but its ref is `main`.

Why reviewers are on `prod-approval` and not on `prod`: reviewers on `prod` would block the
unattended daily run (ADR 0025).

## 4. GitHub Pages

Settings, Pages, Build and deployment, Source: **GitHub Actions**. GitHub creates the
`github-pages` environment on the first deployment; restrict its deployment branches to `main`.
The site will be at `https://<github user>.github.io/cvm-fundwatch/`.

## 5. Branch protection

Settings, Rules, Rulesets, New branch ruleset:

- **main**: target `main`; require a pull request before merging; require the status checks
  `test` and `dashboard` (the jobs in `ci.yml`; they appear after the first CI run); block force
  pushes; restrict deletions.
- **development** (optional, lighter): require status check `test`; block force pushes.

Promotion to prod is a pull request from `development` to `main`.

## 6. First runs

Actions, etl, Run workflow:

1. Branch `development`, environment `dev`, dry run ticked. Check the job summary and download
   the `lake-dev-...` artifact.
2. Same with dry run unticked. The dev dataset gets its first commit; open it on HF and check
   the dataset viewer shows `daily_flows`.
3. Backfill dev with a few months, oldest first, e.g. `months` = `202001-202012`.
4. Open a pull request `development` to `main`, merge when CI is green.
5. Branch `main`, environment `prod`, dry run ticked; then unticked. The unticked run waits for
   your approval in `prod-approval`.
6. Backfill prod in chronological order, one dispatch per range, for example
   `200001-200912`, `201001-201912`, `202001-202512`. Each range is split into commits of 12
   months. Run them oldest first: gold of the first month of a range reads silver of the month
   before, which the previous range published.

7. Once dev has at least 10 months of history (6 training months, 3 history months, the
   evaluation month), run Actions, train, environment `dev`. The first model is published when
   it respects the alarm budget. Then the same for `prod`; publishing waits for your approval.

From then on the ETL runs Monday to Saturday at 17:23 UTC for both environments, with a 13 month
lookback on Saturdays, and training runs on Sundays at 06:17 UTC. Until a model exists, the
Isolation Forest layer is off and everything else works.

## 7. Local runs against Hugging Face

```bash
export HF_OWNER=<owner>
export HF_TOKEN=<dev token>
uv run cvm-lake run --env dev --target hf --out data/hf-dev --dry-run
```

Outside GitHub Actions the guard skips the `CVM_LAKE_ENV` and branch checks, but the repo name,
token and visibility checks still apply.
