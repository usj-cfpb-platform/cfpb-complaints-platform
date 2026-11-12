<#
.SYNOPSIS
  Creates the GitHub labels, milestones, project board and Turn 1-3 issues for the team.

.DESCRIPTION
  Safe to re-run: existing labels are updated, existing milestones and issues (same title)
  are skipped. Requires the GitHub CLI (gh), logged in with the "project" scope:
      gh auth login
      gh auth refresh -s project

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\github\bootstrap_project.ps1
#>
param(
    [string]$Org = "usj-cfpb-platform",
    [string]$Repo = "usj-cfpb-platform/cfpb-complaints-platform",
    [string]$ProjectTitle = "CFPB Platform - 14-day plan"
)

$ErrorActionPreference = "Stop"
$Utf8NoBom = New-Object System.Text.UTF8Encoding $false

function Invoke-Gh {
    $output = & gh @args
    if ($LASTEXITCODE -ne 0) { throw "gh failed: gh $($args -join ' ')" }
    return $output
}

# ---------------------------------------------------------------- labels
$labels = @(
    @{ n = "task";             c = "0E8A16"; d = "Planned unit of work" },
    @{ n = "bug";              c = "D73A4A"; d = "Defect (feeds the defect log)" },
    @{ n = "docs";             c = "0075CA"; d = "Documentation or report" },
    @{ n = "test";             c = "BFD4F2"; d = "Tests" },
    @{ n = "risk";             c = "B60205"; d = "Tracked project risk" },
    @{ n = "domain-needed";    c = "FBCA04"; d = "Blocked until the lecturer assigns our domain" },
    @{ n = "layer:infra";      c = "5319E7"; d = "Docker, CI, repository" },
    @{ n = "layer:ingestion";  c = "1D76DB"; d = "Collectors and raw zone" },
    @{ n = "layer:processing"; c = "0052CC"; d = "Spark and data quality" },
    @{ n = "layer:database";   c = "006B75"; d = "MongoDB serving layer" },
    @{ n = "layer:analysis";   c = "C2E0C6"; d = "Analytical questions and measures" },
    @{ n = "layer:dashboard";  c = "F9D0C4"; d = "Streamlit" }
)
1..6 | ForEach-Object { $labels += @{ n = "turn:$_"; c = "EDEDED"; d = "Work of turn $_" } }

Write-Host "Labels..."
foreach ($l in $labels) {
    Invoke-Gh label create $l.n --repo $Repo --color $l.c --description $l.d --force | Out-Null
}

# ---------------------------------------------------------------- milestones
$W1 = "Week 1 - pipeline end-to-end"
$W2 = "Week 2 - final release"
$milestones = @(
    @{ t = $W1; due = "2026-11-18T21:59:59Z"; d = "Sample data flows raw -> curated -> MongoDB -> dashboard; scheduler collecting real runs." },
    @{ t = $W2; due = "2026-11-24T21:59:59Z"; d = "Analysis, dashboard, tests, report and rehearsed demo. Demo on 2026-11-25." }
)
Write-Host "Milestones..."
$existingMilestones = @(Invoke-Gh api "repos/$Repo/milestones?state=all&per_page=100" --jq ".[].title")
foreach ($m in $milestones) {
    if ($existingMilestones -contains $m.t) { Write-Host "  exists: $($m.t)"; continue }
    Invoke-Gh api "repos/$Repo/milestones" -f "title=$($m.t)" -f "due_on=$($m.due)" -f "description=$($m.d)" | Out-Null
    Write-Host "  created: $($m.t)"
}

# ---------------------------------------------------------------- project board
Write-Host "Project board..."
$projects = Invoke-Gh project list --owner $Org --format json | ConvertFrom-Json
$project = $projects.projects | Where-Object { $_.title -eq $ProjectTitle } | Select-Object -First 1
if (-not $project) {
    $project = Invoke-Gh project create --owner $Org --title $ProjectTitle --format json | ConvertFrom-Json
    Write-Host "  created project #$($project.number)"
}
$projectNumber = $project.number
try { & gh project link $projectNumber --owner $Org --repo $Repo 2>$null | Out-Null } catch { }

# ---------------------------------------------------------------- issues
$issues = @(
    @{ t = "Add Maroun and Ali to CODEOWNERS and require code-owner review"; l = "task,layer:infra,turn:1"; m = $W1; b = @'
## Goal
Every PR automatically requests the right layer owner as reviewer.

## Definition of done
- [ ] Both usernames replace the placeholders in `.github/CODEOWNERS`
- [ ] Every path has at least two owners
- [ ] Branch rule on `main`: "Require review from Code Owners" enabled
'@ },
    @{ t = "Merge Turn 1 PRs after review and tag v0.1-day3"; l = "task,layer:infra,turn:1"; m = $W1; b = @'
## Goal
Turn 1 foundation is on `main` so Turn 2 builds on it.

## Definition of done
- [ ] Skeleton, CI, API probe, contracts and Docker Compose PRs reviewed with real comments and merged
- [ ] `docker compose up -d --build` works from a fresh clone; Spark smoke test passes
- [ ] Release tag `v0.1-day3` with short notes
'@ },
    @{ t = "Historical loader: download the full CSV ZIP into MinIO with a manifest"; l = "task,layer:ingestion,turn:2"; m = $W1; b = @'
## Goal
Scripted, repeatable historical load (brief 8.1).

## Definition of done
- [ ] Downloads https://files.consumerfinance.gov/ccdb/complaints.csv.zip with a resumable, retrying method
- [ ] Original ZIP and extracted CSV stored under `raw/cfpb/full_load/ingest_date=.../run_id=.../` (see `docs/data_contracts.md`)
- [ ] `manifest.json` written and valid against `config/schemas/manifest.schema.json`
- [ ] Source file never edited; rerun creates a new `run_id`
- [ ] A second member repeats the load from an empty environment using only the README
'@ },
    @{ t = "Risk: full CSV download on a slow connection"; l = "risk,layer:ingestion,turn:2"; m = $W1; b = @'
## Context
API `_meta.total_record_count` is about 19.1 million. Measured download speed on one laptop is about 75 kB/s.

## Definition of done
- [ ] Measure the ZIP size (HTTP HEAD, Content-Length) and estimate download time per member
- [ ] Choose who downloads, where and when (fastest connection, overnight, resumable)
- [ ] Record the decision in `docs/architecture.md` decision log
'@ },
    @{ t = "Incremental API collector: window, overlap, search_after, retries, run metadata"; l = "task,layer:ingestion,turn:2"; m = $W1; b = @'
## Goal
Incremental collection (brief 5.2, 8.2) following `docs/data_contracts.md`.

## Definition of done
- [ ] Window `[HWM - lookback_days, today]`, server-side product filter from `config/settings.yaml`
- [ ] Pagination with `search_after` keys from `_meta.break_points`; verify that `frm` offsets repeat page 1
- [ ] Timeouts >= 60 s (probe measured 21 s for 5 records), bounded retries with backoff, no infinite retry
- [ ] Every page saved raw before transformation; `run_metadata.json` valid against the schema
- [ ] High-water mark advances only on success
'@ },
    @{ t = "Scheduled collection on GitHub Actions every 12 h, with manual trigger"; l = "task,layer:ingestion,layer:infra,turn:2"; m = $W1; b = @'
## Goal
Real scheduled runs accumulate from day 5 until the demo (decision D-002).

## Definition of done
- [ ] Workflow on cron at an off-hour minute plus `workflow_dispatch`, same code as manual runs
- [ ] Raw pages and run metadata kept as workflow artifacts
- [ ] Running on `main` by day 5 at the latest
'@ },
    @{ t = "Sync collected runs from GitHub artifacts into the MinIO raw zone"; l = "task,layer:ingestion,turn:2"; m = $W1; b = @'
## Goal
Raw API data collected on GitHub lands in MinIO unchanged.

## Definition of done
- [ ] Command downloads new run artifacts and uploads them to `raw/cfpb/api/...` with the original `run_id`
- [ ] Checksums verified against `run_metadata.json`; already-synced runs skipped
'@ },
    @{ t = "Development sample and sampling script"; l = "task,layer:ingestion,turn:2"; m = $W1; b = @'
## Goal
Small committed sample so tests and CI never need the full dataset (brief 8.7).

## Definition of done
- [ ] Script produces a small CSV sample and a few API pages in `data/sample/`
- [ ] Sample includes edge cases: missing state, bad date, duplicate complaint_id
'@ },
    @{ t = "Collector tests with a mock API"; l = "test,layer:ingestion,turn:2"; m = $W1; b = @'
## Definition of done (brief 14)
- [ ] Known date window
- [ ] Pagination across several pages
- [ ] HTTP error (500 then success; 404 fails fast)
- [ ] Timeout
- [ ] Empty response
- [ ] Repeated request does not create duplicate raw objects or move the high-water mark wrongly
'@ },
    @{ t = "Verify CSV header and API date formats; update the data dictionary"; l = "docs,layer:ingestion,turn:2"; m = $W1; b = @'
## Definition of done
- [ ] Real CSV column names recorded in `docs/data_dictionary.md`
- [ ] Date format in API responses (ISO or epoch) documented, with an example
'@ },
    @{ t = "Turn 3 prep: review contracts and draft quality thresholds"; l = "task,layer:processing,turn:3"; m = $W1; b = @'
## Goal
Ali is ready to start Spark processing on day 5.

## Definition of done
- [ ] Review comments on the contracts PR (types, derived fields, dedup tie-break)
- [ ] Draft `config/quality_rules.yaml` (pass/warn/fail per dimension), to be committed before the final run
'@ }
)

Write-Host "Issues..."
$existingTitles = @(Invoke-Gh issue list --repo $Repo --state all --limit 300 --json title --jq ".[].title")
foreach ($i in $issues) {
    if ($existingTitles -contains $i.t) { Write-Host "  exists: $($i.t)"; continue }
    $bodyFile = [System.IO.Path]::GetTempFileName()
    [System.IO.File]::WriteAllText($bodyFile, $i.b, $Utf8NoBom)
    $url = Invoke-Gh issue create --repo $Repo --title $i.t --body-file $bodyFile --label $i.l --milestone $i.m
    Remove-Item $bodyFile
    Invoke-Gh project item-add $projectNumber --owner $Org --url $url | Out-Null
    Write-Host "  created: $url"
}

Write-Host "Done. Board: https://github.com/orgs/$Org/projects/$projectNumber"
