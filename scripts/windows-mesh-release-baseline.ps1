# Hosted-only #9569 T11/T12: reuse the exact built base for rollout, then run one identical benchmark driver on base/head.
$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT' -or $env:GITHUB_ACTIONS -ne 'true') { throw 'Requires hosted Windows CI; no local benchmark' }
$base = 'dcc0825cb005ebfc37c37514f159cdb3570281f4'
$checkout = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$head = (& git -C $checkout rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $head -notmatch '^[a-f0-9]{40}$') { throw 'Cannot resolve candidate HEAD' }
if (-not $env:RUNNER_TEMP -or -not (Test-Path -LiteralPath $env:FABRIC_WIN_ZIG -PathType Leaf)) { throw 'RUNNER_TEMP and installed FABRIC_WIN_ZIG are required' }
$stage = Join-Path $env:RUNNER_TEMP ('f9569-' + [Guid]::NewGuid().ToString('N').Substring(0, 12))
$null = New-Item -ItemType Directory -Path $stage
# Private project, not a worktree/checkout mutation or a shared dependency install.
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
& "$env:SystemRoot\System32\icacls.exe" $stage /inheritance:r /grant:r "${identity}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F'
if ($LASTEXITCODE -ne 0) { throw 'Cannot make baseline project private' }
$project = Join-Path $stage 'b'
$results = Join-Path $stage 'results'
$temp = Join-Path $stage 't'
$null = New-Item -ItemType Directory -Path $project, $results, $temp
"FABRIC_9569_PERF_RESULTS=$results" | Out-File -FilePath $env:GITHUB_ENV -Append -Encoding utf8
$archive = Join-Path $stage 'base.tar'
& git -C $checkout archive --format=tar "--output=$archive" $base
if ($LASTEXITCODE -ne 0) { throw "Pinned base $base unavailable" }
& "$env:SystemRoot\System32\tar.exe" -xf $archive -C $project
if ($LASTEXITCODE -ne 0) { throw 'Baseline archive extraction failed' }
$baseLock = (Get-FileHash (Join-Path $project 'bun.lock') -Algorithm SHA256).Hash.ToLowerInvariant()
$headLock = (Get-FileHash (Join-Path $checkout 'bun.lock') -Algorithm SHA256).Hash.ToLowerInvariant()
if ($baseLock -ne $headLock) { throw 'Base/head bun.lock differ: cannot reuse installed dependencies; no install attempted' }
# The exact-lock existing install is read through a junction; never run install or change global config.
$dependencies = (Resolve-Path (Join-Path $checkout 'node_modules')).Path
$null = New-Item -ItemType Junction -Path (Join-Path $project 'node_modules') -Target $dependencies
$oldTemp = $env:TEMP; $oldTmp = $env:TMP; $oldTmpdir = $env:TMPDIR
$oldZigGlobal = $env:ZIG_GLOBAL_CACHE_DIR; $oldZigLocal = $env:ZIG_LOCAL_CACHE_DIR
try {
    $env:TEMP = $temp; $env:TMP = $temp; $env:TMPDIR = $temp
    $env:ZIG_GLOBAL_CACHE_DIR = Join-Path $stage 'zig-global'
    $env:ZIG_LOCAL_CACHE_DIR = Join-Path $stage 'zig-local'
    Push-Location $project
    try {
        # Same pinned Windows zig as the candidate build; the base checks its own expected native digest.
        & bun run build 2>&1 | Tee-Object -FilePath (Join-Path $results 'baseline-build.log')
        if ($LASTEXITCODE -ne 0) { throw 'Pinned baseline build failed' }
    } finally { Pop-Location }
    # T11 uses the ACTUAL pinned baseline just built above, never a hand-written old-release facsimile.
    # The ordinary affected/smoke invocation skips T11 until this registration supplies its old/current dist inputs.
    $rolloutEnv = @{
        FABRIC_9569_ROLLOUT = '1'
        FABRIC_9569_ROLLOUT_BASE_PROJECT = $project
        FABRIC_9569_ROLLOUT_BASE_HEAD = $base
        FABRIC_9569_ROLLOUT_CURRENT_PROJECT = $checkout
        FABRIC_9569_ROLLOUT_CURRENT_HEAD = $head
        FABRIC_9569_ROLLOUT_RESULTS = $results
    }
    $savedRolloutEnv = @{}
    foreach ($key in $rolloutEnv.Keys) {
        $savedRolloutEnv[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
        [Environment]::SetEnvironmentVariable($key, $rolloutEnv[$key], 'Process')
    }
    Push-Location $checkout
    try {
        & bunx vitest run --retry=0 tests/mesh-lock-windows-rollout.test.ts 2>&1 | Tee-Object -FilePath (Join-Path $results 'rollout-probe.log')
        $rolloutExit = $LASTEXITCODE
    } finally {
        Pop-Location
        foreach ($key in $rolloutEnv.Keys) { [Environment]::SetEnvironmentVariable($key, $savedRolloutEnv[$key], 'Process') }
    }
    # Preserve T12's identical driver, baseline-then-candidate order and 100 + 4x25 operations, even if T11 failed.
    $driver = Join-Path $checkout 'scripts/benchmark-windows-mesh-release.mjs'
    $baseReceipt = Join-Path $results 'baseline.json'
    $headReceipt = Join-Path $results 'candidate.json'
    & node $driver --project $project --label baseline --head $base --out $baseReceipt 2>&1 | Tee-Object -FilePath (Join-Path $results 'baseline-probe.log')
    $baseExit = $LASTEXITCODE
    # Run the candidate even if the completed baseline reports residue/failure; never rerun application work.
    & node $driver --project $checkout --label candidate --head $head --out $headReceipt 2>&1 | Tee-Object -FilePath (Join-Path $results 'candidate-probe.log')
    $headExit = $LASTEXITCODE
    if (-not (Test-Path $baseReceipt) -or -not (Test-Path $headReceipt)) { throw 'Missing benchmark receipt; see phase logs' }
    $b = Get-Content -LiteralPath $baseReceipt -Raw | ConvertFrom-Json
    $h = Get-Content -LiteralPath $headReceipt -Raw | ConvertFrom-Json
    function Delta($before, $after) {
        if ($null -eq $before -or $null -eq $after) { return $null }
        $percent = if ($before -ne 0) { 100 * ($after - $before) / $before } else { $null }
        return @{ baselineMs = $before; candidateMs = $after; deltaMs = $after - $before; deltaPercent = $percent }
    }
    $comparison = [ordered]@{
        schema = 'fabric-windows-mesh-release-comparison/1'
        order = @('baseline', 'candidate')
        baselineHead = $base; candidateHead = $head; lockSha256 = $headLock
        sameRunner = $env:RUNNER_NAME; runId = $env:GITHUB_RUN_ID; attempt = $env:GITHUB_RUN_ATTEMPT
        driverSha256 = (Get-FileHash $driver -Algorithm SHA256).Hash.ToLowerInvariant()
        baselineArchiveSha256 = (Get-FileHash $archive -Algorithm SHA256).Hash.ToLowerInvariant()
        zig = $env:FABRIC_WIN_ZIG; dependencySource = $dependencies; privateProject = $project
        baselineArtifacts = $b.artifacts; candidateArtifacts = $h.artifacts
        baselineExit = $baseExit; candidateExit = $headExit
        serial = @{
            p50 = (Delta $b.serial.sections.p50Ms $h.serial.sections.p50Ms)
            p95 = (Delta $b.serial.sections.p95Ms $h.serial.sections.p95Ms)
            sampleTotal = (Delta $b.serial.sections.totalMs $h.serial.sections.totalMs)
            total = (Delta $b.serial.totalMs $h.serial.totalMs)
        }
        fourProcessContention = @{
            p50 = (Delta $b.fourProcessContention.sections.p50Ms $h.fourProcessContention.sections.p50Ms)
            p95 = (Delta $b.fourProcessContention.sections.p95Ms $h.fourProcessContention.sections.p95Ms)
            sampleTotal = (Delta $b.fourProcessContention.sections.totalMs $h.fourProcessContention.sections.totalMs)
            joinedWall = (Delta $b.fourProcessContention.wallMs $h.fourProcessContention.wallMs)
        }
        releaseChildren = @{ baseline = $b.serial.releaseChildren.status; candidateSerial = $h.serial.releaseChildren.timing; candidateContended = $h.fourProcessContention.releaseChildren.timing }
        nativeCensus = 'Actual PID/UTC Get-Process handle and direct-child snapshots are in each receipt, including all four workers; report-only, not peak or total process counts.'
        nativeHandleDeltas = @{
            serial = @{ baseline = $b.serial.nativeHandleCensus.delta; candidate = $h.serial.nativeHandleCensus.delta }
            contentionController = @{ baseline = $b.fourProcessContention.nativeHandleCensus.delta; candidate = $h.fourProcessContention.nativeHandleCensus.delta }
            contentionWorkers = @{
                baseline = @($b.fourProcessContention.workers | ForEach-Object { @{ pid = $_.pid; delta = $_.nativeHandleCensus.delta } })
                candidate = @($h.fourProcessContention.workers | ForEach-Object { @{ pid = $_.pid; delta = $_.nativeHandleCensus.delta } })
            }
        }
        residue = @{ baseline = $b.retirementResidue; candidate = $h.retirementResidue; baselineAllPaths = @($b.leftovers); candidateAllPaths = @($h.leftovers) }
        receipts = @{ baseline = $baseReceipt; candidate = $headReceipt }
        interpretation = 'Timing/handle deltas are report-only; baseline release children are not-instrumented, never assumed zero. Candidate requires exactly 100 serial + 100 contended joined release children. No performance acceptance threshold chosen before evidence.'
    }
    $json = $comparison | ConvertTo-Json -Depth 30
    $comparisonPath = Join-Path $results 'comparison.json'
    $json | Set-Content -LiteralPath $comparisonPath -Encoding utf8
    Write-Output "WINDOWS_MESH_RELEASE_COMPARISON: $json"
    if ($env:GITHUB_STEP_SUMMARY) {
        @('## Windows mesh release T12 (same runner, base then head)', '', '```json', $json, '```') | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
    }
    Write-Output "#9569 receipts: $results"
    if ($baseExit -ne 0 -or $headExit -ne 0) { throw "Benchmark correctness/instrumentation failure: baseline=$baseExit candidate=$headExit (all worker close events joined)" }
    if ($rolloutExit -ne 0) { throw "T11 pinned-old rollout/drain/canary failed: exit=$rolloutExit; see rollout-probe.log and t11-p*.json" }
} finally {
    $env:TEMP = $oldTemp; $env:TMP = $oldTmp; $env:TMPDIR = $oldTmpdir
    $env:ZIG_GLOBAL_CACHE_DIR = $oldZigGlobal; $env:ZIG_LOCAL_CACHE_DIR = $oldZigLocal
}
# Keep the isolated project and receipts for this disposable hosted job; no shared-root sweeper/cleanup.
