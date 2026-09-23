<#
.SYNOPSIS
    Exercises the *total* partition-loss path (Solace's FlowInactive/FlowActive) that
    run-benchmark-mid-drain-rebalance.ps1 cannot reach: starts one instance per partition, lets them
    build real unacked backlog, then makes exactly one of them simulate a network blip (via
    Subscriber:SimulateBlipAfterSeconds/SimulateBlipDurationSeconds) while it's still bound and busy.
    Writes a self-contained HTML report to reports/.

.DESCRIPTION
    Two probes run this session established why this script looks the way it does:

    1. Adding more consumers than a partitioned queue has partitions never displaces an existing
       owner - the new joiners just stay permanently idle (see run-benchmark-mid-drain-rebalance.ps1's
       notes on the partial-rebalance case, and this session's own probe: 8 instances holding 1
       partition each kept them untouched when 4 more joined). So the only way for an
       already-active, still-running instance to reach FlowEvent.FlowInactive is a genuine
       disconnect - not something reachable by just starting more processes.

    2. Reducing the queue's own partition count *would* force this, but Solace's docs say doing so
       disconnects every bound client and can drop in-flight messages - too destructive for a
       repeatable local benchmark.

    So this script uses SolaceConcurrentSubscriber<T>'s own chaos-testing knob instead:
    Subscriber:SimulateBlipAfterSeconds makes one instance deliberately disconnect and reconnect its
    own session mid-run (see NetworkBlipSimulatorService). Confirmed empirically before this script
    was written: the blip instance logs "is inactive (holds no partitions)" the instant it
    disconnects (its generation counter bumps immediately - see SolaceConcurrentSubscriber<T>'s
    OnFlowEvent/_generation), and typically logs "is active" again ~5s (the queue's
    partitionRebalanceDelay) after reconnecting, once the broker re-admits it as a bound consumer.

    What actually gets measured:
      - "Dropped" - how many buffered messages the blip instance's own generation check discarded
        instead of handling, parsed from its "Dropped a buffered message..." log line (see
        SolaceConcurrentSubscriber<T>.LogDropped). This is the fix engaging, directly observed.
      - "Global duplicates"/"Global reorders" - the same cross-process HANDLED-log merge
        run-benchmark-mid-drain-rebalance.ps1 uses. A non-zero count here after a Dropped count > 0
        would mean the fix didn't fully prevent double-handling; some residual risk is expected even
        so (see notes below) - a message already inside a synchronous Handle() call when the blip
        hits can't be aborted, only ones still buffered ahead of it are caught by the generation check.

.PARAMETER PartitionCount
    The partition count the target queue (-Queue) was created with. -InstanceCount defaults to this,
    since one partition per instance is what makes the blip instance's partition loss unambiguous
    (see the script header's probe notes on idle-consumer stickiness).

.PARAMETER InstanceCount
    Subscriber instances started, each bound to the same partitioned queue. Defaults to
    -PartitionCount.

.PARAMETER BlipInstanceId
    Which instance (1-based) simulates the network blip.

.PARAMETER BlipAfterSeconds
    Seconds after that instance starts before it disconnects. Short enough that it should still have
    real unacked backlog at that point, given -Count/-PartitionCount and the subscriber's
    SimulatedHandlerWork*Ms.

.PARAMETER BlipDurationSeconds
    How long the simulated blip lasts. Needs to comfortably exceed the queue's
    partitionRebalanceDelay (5s by default) so the vacated partition is actually reassigned before
    the blip instance reconnects - confirmed empirically at 10s during this script's development.

.PARAMETER Concurrency
    Per-instance lane count (SolaceConcurrentSubscriber<T> concurrency), uniform across all instances.

.PARAMETER Count / KeyCount
    Messages published, and distinct OrderId keys, round-robin.

.PARAMETER Trials
    How many times to repeat the whole scenario (fresh publish each time). Rebalance/blip timing is a
    wall-clock race, so more than one trial is worth running before trusting a single result - see
    run-benchmark-mid-drain-rebalance.ps1's own notes on why its single-trial results were noisy.

.PARAMETER Queue / Topic
    The partitioned queue and the topic it's subscribed to. Defaults match
    run-benchmark-multisubscriber.ps1's CHANGED-P / data-changed-p.

.PARAMETER DrainTimeoutSeconds
    Max seconds to wait for a trial's backlog to fully drain before giving up on it. Defaults (0) to
    a value scaled from Count and the subscriber's SimulatedHandlerWorkMaxMs, plus the blip's own
    downtime, as in the other multi-subscriber benchmarks.

.PARAMETER SempBaseUrl / SempUser / SempPassword
    Broker admin SEMP endpoint used to poll queue depth. Defaults match the local Docker broker
    documented in scripts/start-solace.ps1.

.EXAMPLE
    ./scripts/run-benchmark-blip-rebalance.ps1 -PartitionCount 8

.EXAMPLE
    ./scripts/run-benchmark-blip-rebalance.ps1 -PartitionCount 8 -Trials 5 -Count 800
#>

param(
    [int]$PartitionCount = 8,
    [int]$InstanceCount = 0,
    [int]$BlipInstanceId = 1,
    [int]$BlipAfterSeconds = 4,
    [int]$BlipDurationSeconds = 10,
    [int]$Concurrency = 4,
    [int]$WindowSize = 0,
    [int]$Count = 400,
    [int]$KeyCount = 64,
    [int]$Trials = 1,
    [string]$Queue = "CHANGED-P",
    [string]$Topic = "data-changed-p",
    [int]$DrainTimeoutSeconds = 0,
    [string]$SempBaseUrl = "http://localhost:8080",
    [string]$SempUser = "admin",
    [string]$SempPassword = "admin"
)

$ErrorActionPreference = "Stop"

if ($InstanceCount -le 0) { $InstanceCount = $PartitionCount }
if ($BlipInstanceId -lt 1 -or $BlipInstanceId -gt $InstanceCount) {
    Write-Error "-BlipInstanceId ($BlipInstanceId) must be between 1 and -InstanceCount ($InstanceCount)."
    exit 1
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$SubscriberDir = Join-Path $RepoRoot "messaging-lab.solace.subscriber"
$LoadgenDir = Join-Path $RepoRoot "messaging-lab.solace.loadgen"
$LogsDir = Join-Path $SubscriberDir "bin/Debug/net10.0/logs"

$AppSettings = Get-Content (Join-Path $SubscriberDir "appsettings.json") -Raw | ConvertFrom-Json
$MetricsReportIntervalSeconds = [int]$AppSettings.Subscriber.MetricsReportIntervalSeconds

if ($DrainTimeoutSeconds -le 0) {
    $worstCaseSeconds = ($Count * $AppSettings.Subscriber.SimulatedHandlerWorkMaxMs / 1000) + $BlipAfterSeconds + $BlipDurationSeconds
    $DrainTimeoutSeconds = [Math]::Max(300, [int]($worstCaseSeconds * 1.5))
    Write-Host "DrainTimeoutSeconds not specified - defaulting to ${DrainTimeoutSeconds}s."
}

$AuthHeader = @{
    Authorization = "Basic " + [Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("${SempUser}:${SempPassword}"))
}
$QueueMonitorUrl = "$SempBaseUrl/SEMP/v2/monitor/msgVpns/default/queues/$Queue"

function Get-QueueSpoolUsage {
    (Invoke-RestMethod -Uri $QueueMonitorUrl -Headers $AuthHeader).data.msgSpoolUsage
}

# --- Preconditions ------------------------------------------------------------

try {
    $q = Invoke-RestMethod -Uri $QueueMonitorUrl -Headers $AuthHeader
}
catch {
    Write-Error "Could not reach queue '$Queue' via SEMP at $QueueMonitorUrl. Is the broker running (./scripts/start-solace.ps1), and does '$Queue' exist as a non-exclusive, partitioned queue subscribed to topic '$Topic'? See README > Comparing multiple subscribers."
    exit 1
}
if ($q.data.msgSpoolUsage -ne 0) {
    Write-Error "Queue '$Queue' is not empty (msgSpoolUsage=$($q.data.msgSpoolUsage)). Drain it before benchmarking so results aren't mixed with leftover messages."
    exit 1
}

Write-Host "$InstanceCount instance(s) (1 per partition), instance $BlipInstanceId blips after ${BlipAfterSeconds}s for ${BlipDurationSeconds}s, $Concurrency lane(s)/instance, $Trials trial(s)."

# --- Build once, up front -----------------------------------------------------

Write-Host "`n=== Building loadgen and subscriber (Debug) ==="
dotnet build (Join-Path $LoadgenDir "messaging-lab.solace.loadgen.csproj") -c Debug | Out-Null
dotnet build (Join-Path $SubscriberDir "messaging-lab.solace.subscriber.csproj") -c Debug | Out-Null

$LoadgenDll = Join-Path $LoadgenDir "bin/Debug/net10.0/messaging-lab.solace.loadgen.dll"
$SubscriberDll = Join-Path $SubscriberDir "bin/Debug/net10.0/messaging-lab.solace.subscriber.dll"
if (-not (Test-Path $LoadgenDll)) { Write-Error "Expected build output not found: $LoadgenDll"; exit 1 }
if (-not (Test-Path $SubscriberDll)) { Write-Error "Expected build output not found: $SubscriberDll"; exit 1 }

# --- Helpers -------------------------------------------------------------------

function Publish-Batch {
    Push-Location $LoadgenDir
    try {
        dotnet $LoadgenDll --LoadGen:Topic $Topic --LoadGen:Count $Count --LoadGen:KeyCount $KeyCount | Out-Null
    }
    finally {
        Pop-Location
    }
}

function Start-SubscriberJob([int]$InstanceId, [bool]$IsBlipInstance) {
    Start-Job -ScriptBlock {
        param($subscriberDir, $dll, $queue, $concurrency, $windowSize, $instanceId, $isBlip, $blipAfter, $blipDuration)
        Set-Location $subscriberDir
        $cliArgs = @(
            "--Subscriber:Queue", $queue,
            "--Subscriber:UseConcurrentSubscriber", "true",
            "--Subscriber:Concurrency", $concurrency,
            "--Subscriber:InstanceId", $instanceId
        )
        if ($windowSize -gt 0) {
            $cliArgs += @("--Subscriber:WindowSize", $windowSize)
        }
        if ($isBlip) {
            $cliArgs += @("--Subscriber:SimulateBlipAfterSeconds", $blipAfter, "--Subscriber:SimulateBlipDurationSeconds", $blipDuration)
        }
        dotnet $dll @cliArgs
    } -ArgumentList $SubscriberDir, $SubscriberDll, $Queue, $Concurrency, $WindowSize, $InstanceId, $IsBlipInstance, $BlipAfterSeconds, $BlipDurationSeconds
}

$MetricsPattern = 'Handled (\d+) messages in ([\d:.]+) \(([\d.]+) msgs/sec\) - ordering violations: (\d+), duplicates: (\d+), latency p50=([\d.]+)ms p99=([\d.]+)ms'
$HandledPattern = '^(?<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [+\-]\d{2}:\d{2})\s+\[INF\]\s+\[[^\]]*\][^:]*:\s*HANDLED OrderId=(?<oid>\S+) Sequence=(?<seq>\d+)'
$DroppedPattern = 'Dropped a buffered message'
$FlowInactivePattern = "is inactive \(holds no partitions\)"
$FlowActivePattern = "is active \(holds at least one partition\)"

function Get-InstanceLogFile([int]$InstanceId) {
    Get-ChildItem -Path $LogsDir -Filter "subscriber-$InstanceId-*.log" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
}

# Same technique as run-benchmark-mid-drain-rebalance.ps1's Get-HandledEvents: only a cross-process,
# timestamp-merged view of every instance's HANDLED log line can see a message the blip instance
# handled once and whichever consumer inherited its partition handled again.
function Get-HandledEvents {
    param([int]$InstanceId, [datetime]$Start, [datetime]$End)

    $file = Get-InstanceLogFile -InstanceId $InstanceId
    if (-not $file) { return @() }

    $windowStart = $Start.AddSeconds(-1)
    $windowEnd = $End.AddSeconds(2)

    Get-Content -Path $file.FullName | ForEach-Object {
        $m = [regex]::Match($_, $HandledPattern)
        if (-not $m.Success) { return }

        $ts = [DateTimeOffset]::Parse($m.Groups['ts'].Value).LocalDateTime
        if ($ts -lt $windowStart -or $ts -gt $windowEnd) { return }

        [pscustomobject]@{
            Timestamp = $ts
            OrderId   = $m.Groups['oid'].Value
            Sequence  = [long]$m.Groups['seq'].Value
        }
    }
}

function Measure-GlobalOrdering {
    param([int]$InstanceCount, [datetime]$Start, [datetime]$End)

    $allEvents = 1..$InstanceCount | ForEach-Object { Get-HandledEvents -InstanceId $_ -Start $Start -End $End }
    $sorted = @($allEvents | Sort-Object Timestamp)

    $lastSeq = @{}
    $duplicates = 0
    $outOfOrder = 0
    foreach ($e in $sorted) {
        if ($lastSeq.ContainsKey($e.OrderId)) {
            if ($e.Sequence -eq $lastSeq[$e.OrderId]) { $duplicates++ }
            elseif ($e.Sequence -lt $lastSeq[$e.OrderId]) { $outOfOrder++ }
            else { $lastSeq[$e.OrderId] = $e.Sequence }
        }
        else {
            $lastSeq[$e.OrderId] = $e.Sequence
        }
    }

    [pscustomobject]@{ EventCount = $sorted.Count; Duplicates = $duplicates; OutOfOrder = $outOfOrder }
}

# Counts lines matching $pattern in the blip instance's log within [$Start, $End] - used for the
# "Dropped" count and to confirm the FlowInactive/FlowActive transitions actually happened.
function Measure-BlipInstanceLines {
    param([string]$Pattern, [datetime]$Start, [datetime]$End)

    $file = Get-InstanceLogFile -InstanceId $BlipInstanceId
    if (-not $file) { return 0 }

    $windowStart = $Start.AddSeconds(-1)
    $windowEnd = $End.AddSeconds(2)
    $count = 0
    Get-Content -Path $file.FullName | ForEach-Object {
        if ($_ -notmatch $Pattern) { return }
        $tsMatch = [regex]::Match($_, '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [+\-]\d{2}:\d{2}')
        if (-not $tsMatch.Success) { return }
        $ts = [DateTimeOffset]::Parse($tsMatch.Value).LocalDateTime
        if ($ts -ge $windowStart -and $ts -le $windowEnd) { $count++ }
    }
    $count
}

# Publishes a batch, starts $InstanceCount instances (one of them configured to blip), waits for the
# broker to report the queue fully drained, then stops everything and returns per-instance, global,
# and blip-instance-specific counts for this trial.
function Invoke-BlipTrial {
    Publish-Batch
    $configStart = Get-Date

    $jobs = 1..$InstanceCount | ForEach-Object {
        Start-SubscriberJob -InstanceId $_ -IsBlipInstance:($_ -eq $BlipInstanceId)
    }

    $drainStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $waited = 0
    while ($waited -lt $DrainTimeoutSeconds) {
        Start-Sleep -Seconds 1
        $waited++
        if ((Get-QueueSpoolUsage) -eq 0) { break }
    }
    $drainStopwatch.Stop()
    $configEnd = Get-Date
    if ($waited -ge $DrainTimeoutSeconds) {
        $jobs | Stop-Job | Out-Null
        $jobs | Remove-Job -Force
        throw "Timed out after ${DrainTimeoutSeconds}s waiting for the trial to drain (queue usage=$(Get-QueueSpoolUsage)). Aborting - increase -DrainTimeoutSeconds and re-run."
    }

    Start-Sleep -Seconds ($MetricsReportIntervalSeconds + 1)
    $jobs | Stop-Job | Out-Null

    $perInstance = foreach ($job in $jobs) {
        $output = Receive-Job -Job $job
        $match = $output | Select-String -Pattern $MetricsPattern | Select-Object -Last 1
        if ($match) {
            $g = $match.Matches[0].Groups
            [pscustomobject]@{
                Messages   = [int]$g[1].Value
                Violations = [int]$g[4].Value
                Duplicates = [int]$g[5].Value
            }
        }
        else {
            [pscustomobject]@{ Messages = 0; Violations = 0; Duplicates = 0 }
        }
    }
    $jobs | Remove-Job -Force

    $global = Measure-GlobalOrdering -InstanceCount $InstanceCount -Start $configStart -End $configEnd
    $dropped = Measure-BlipInstanceLines -Pattern $DroppedPattern -Start $configStart -End $configEnd
    $inactiveEvents = Measure-BlipInstanceLines -Pattern $FlowInactivePattern -Start $configStart -End $configEnd
    $activeEvents = Measure-BlipInstanceLines -Pattern $FlowActivePattern -Start $configStart -End $configEnd

    [pscustomobject]@{
        ElapsedS         = $drainStopwatch.Elapsed.TotalSeconds
        PerInstance      = @($perInstance)
        GlobalEventCount = $global.EventCount
        GlobalDuplicates = $global.Duplicates
        GlobalOutOfOrder = $global.OutOfOrder
        Dropped          = $dropped
        InactiveEvents   = $inactiveEvents
        ActiveEvents     = $activeEvents
    }
}

# --- Run the trials --------------------------------------------------------------

$Results = @()

for ($t = 1; $t -le $Trials; $t++) {
    Write-Host "`n=== Trial ${t}/${Trials}: publishing $Count messages across $KeyCount keys ==="
    $run = Invoke-BlipTrial

    $totalMessages = ($run.PerInstance | Measure-Object -Property Messages -Sum).Sum
    $totalPerProcessViolations = ($run.PerInstance | Measure-Object -Property Violations -Sum).Sum
    $totalPerProcessDuplicates = ($run.PerInstance | Measure-Object -Property Duplicates -Sum).Sum

    if ($totalMessages -ne $Count) {
        Write-Warning "Trial $t reported $totalMessages of $Count published messages handled across all instances - the snapshot may not reflect the full drain."
    }
    if ($run.InactiveEvents -eq 0) {
        Write-Warning "Trial ${t}: instance $BlipInstanceId never logged a FlowInactive transition - the blip may not have landed as expected (check DrainTimeoutSeconds and BlipAfterSeconds against how long the batch actually takes to drain)."
    }

    $Results += [pscustomobject]@{
        Trial                 = $t
        Messages              = $totalMessages
        ElapsedS              = $run.ElapsedS
        Rate                  = if ($run.ElapsedS -gt 0) { $totalMessages / $run.ElapsedS } else { 0.0 }
        Dropped               = $run.Dropped
        InactiveEvents        = $run.InactiveEvents
        ActiveEvents          = $run.ActiveEvents
        GlobalDuplicates      = $run.GlobalDuplicates
        GlobalOutOfOrder      = $run.GlobalOutOfOrder
        PerProcessViolations  = $totalPerProcessViolations
        PerProcessDuplicates  = $totalPerProcessDuplicates
    }

    Write-Host "  Messages=$totalMessages  Dropped=$($run.Dropped)  Inactive/ActiveEvents=$($run.InactiveEvents)/$($run.ActiveEvents)  GlobalDuplicates=$($run.GlobalDuplicates)  GlobalOutOfOrder=$($run.GlobalOutOfOrder)"
}

if ($Results.Count -eq 0) {
    Write-Error "No trial produced a result; nothing to report."
    exit 1
}

# --- Report: assemble HTML ------------------------------------------------------

function Encode([string]$s) { [System.Net.WebUtility]::HtmlEncode("$s") }

$ReportsDir = Join-Path $RepoRoot "reports"
New-Item -ItemType Directory -Path $ReportsDir -Force | Out-Null
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$RunDate = Get-Date -Format "yyyy-MM-dd"
$ReportPath = Join-Path $ReportsDir "benchmark-blip-rebalance-$Timestamp.html"

$totalDropped = ($Results | Measure-Object -Property Dropped -Sum).Sum
$totalGlobalDuplicates = ($Results | Measure-Object -Property GlobalDuplicates -Sum).Sum
$totalGlobalOutOfOrder = ($Results | Measure-Object -Property GlobalOutOfOrder -Sum).Sum
$totalMessagesAll = ($Results | Measure-Object -Property Messages -Sum).Sum
$trialsWithoutInactive = ($Results | Where-Object { $_.InactiveEvents -eq 0 }).Count

$rows = New-Object System.Collections.Generic.List[string]
foreach ($r in $Results) {
    $rows.Add(@"
          <tr>
            <td class="tabular">$($r.Trial)</td>
            <td class="tabular">$($r.Messages)</td>
            <td class="tabular">$([math]::Round($r.ElapsedS, 2))s</td>
            <td class="tabular">$([math]::Round($r.Rate, 1))/s</td>
            <td class="tabular $(if ($r.InactiveEvents -gt 0) { 'violations-ok' } else { 'violations-bad' })">$($r.InactiveEvents) / $($r.ActiveEvents)</td>
            <td class="tabular">$($r.Dropped)</td>
            <td class="tabular $(if ($r.GlobalDuplicates -eq 0) { 'violations-ok' } else { 'violations-bad' })">$($r.GlobalDuplicates)</td>
            <td class="tabular $(if ($r.GlobalOutOfOrder -eq 0) { 'violations-ok' } else { 'violations-bad' })">$($r.GlobalOutOfOrder)</td>
          </tr>
"@)
}

$headlineNote = if ($trialsWithoutInactive -gt 0) {
    "$trialsWithoutInactive of $($Results.Count) trial(s) never observed the blip instance go inactive - the mechanism didn't land as designed in those trials; treat their duplicate/drop counts as inconclusive rather than as evidence either way."
}
elseif ($totalGlobalDuplicates -eq 0) {
    "The blip instance went inactive in every trial and dropped $totalDropped buffered message(s) instead of handling them, with zero global duplicates resulting - the generation-based drop in SolaceConcurrentSubscriber<T> did its job across $($Results.Count) trial(s)."
}
else {
    "The blip instance went inactive and dropped $totalDropped buffered message(s) as designed, but $totalGlobalDuplicates duplicate handling(s) still occurred globally - consistent with the known residual gap: a message already inside a synchronous Handle() call when the blip hits can't be aborted, only ones still queued ahead of it are caught."
}

$html = @"
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Blip Rebalance Benchmark - $Timestamp</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Condensed:wght@500;600;700&family=IBM+Plex+Serif:ital,wght@0,400;0,500;0,600;1,400&family=IBM+Plex+Mono:wght@400;500;600&display=swap">
<style>
  :root {
    color-scheme: light dark;
    --paper: #eef0ed; --surface: #ffffff; --surface-2: #f5f6f3;
    --line: #dde0da; --line-strong: #c7ccc3;
    --ink: #1c2129; --ink-soft: #545d6b; --ink-faint: #7c8492;
    --accent: #a35a16; --baseline: #8b93a0;
    --good: #2f7d52; --good-bg: #e3efe7;
    --bad: #a3341f; --bad-bg: #f3e3df;
    --font-display: "IBM Plex Sans Condensed", "Arial Narrow", sans-serif;
    --font-body: "IBM Plex Serif", Georgia, serif;
    --font-mono: "IBM Plex Mono", ui-monospace, "Courier New", monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --paper: #14171c; --surface: #1b1f26; --surface-2: #20242c;
      --line: #2b313b; --line-strong: #3a4250;
      --ink: #eef0f0; --ink-soft: #9aa3b0; --ink-faint: #7c8794;
      --accent: #e08935; --baseline: #9aa3b0;
      --good: #4caf7d; --good-bg: #1e2f26;
      --bad: #e0685a; --bad-bg: #34211e;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--paper); color: var(--ink); font-family: var(--font-body); line-height: 1.55; -webkit-font-smoothing: antialiased; }
  .page { max-width: 900px; margin: 0 auto; padding: 48px 24px 72px; display: flex; flex-direction: column; gap: 40px; }
  h1, h2 { font-family: var(--font-display); text-wrap: balance; margin: 0; }
  code { font-family: var(--font-mono); font-size: 0.92em; background: var(--surface-2); border: 1px solid var(--line); border-radius: 4px; padding: 0.05em 0.35em; }
  .tabular { font-variant-numeric: tabular-nums; }

  .masthead { display: flex; flex-direction: column; gap: 14px; padding-bottom: 28px; border-bottom: 1px solid var(--line); }
  .eyebrow { margin: 0; font-family: var(--font-mono); font-size: 0.78rem; letter-spacing: 0.09em; text-transform: uppercase; color: var(--accent); }
  .masthead h1 { font-size: clamp(1.9rem, 4vw, 2.5rem); font-weight: 700; letter-spacing: -0.01em; }
  .dek { max-width: 66ch; margin: 0; color: var(--ink-soft); font-size: 1.02rem; }
  .run-meta { margin: 6px 0 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; background: var(--surface); }
  .run-meta > div { padding: 10px 14px; border-left: 1px solid var(--line); min-width: 0; overflow: hidden; }
  .run-meta > div:first-child { border-left: none; }
  .run-meta dt { margin: 0; font-family: var(--font-mono); font-size: 0.68rem; letter-spacing: 0.06em; text-transform: uppercase; color: var(--ink-faint); }
  .run-meta dd { margin: 3px 0 0; font-family: var(--font-mono); font-size: 0.86rem; color: var(--ink); overflow-wrap: anywhere; }

  .headline-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 14px; }
  .stat-tile { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 18px 20px; }
  .stat-tile.status-good { background: var(--good-bg); border-color: var(--good); }
  .stat-tile.status-bad { background: var(--bad-bg); border-color: var(--bad); }
  .stat-value { margin: 0; font-family: var(--font-display); font-weight: 700; font-size: 2.05rem; letter-spacing: -0.01em; }
  .status-good .stat-value { color: var(--good); }
  .status-bad .stat-value { color: var(--bad); }
  .stat-label { margin: 6px 0 0; font-size: 0.84rem; color: var(--ink-soft); line-height: 1.4; }

  section h2 { font-size: 1.25rem; font-weight: 600; margin-bottom: 6px; }
  .chart-caption, .section-intro { max-width: 70ch; color: var(--ink-soft); font-size: 0.92rem; margin: 0 0 16px; }

  .table-scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 10px; }
  table { width: 100%; border-collapse: collapse; background: var(--surface); font-size: 0.86rem; min-width: 720px; }
  thead th { text-align: right; font-family: var(--font-mono); font-weight: 500; font-size: 0.7rem; letter-spacing: 0.05em; text-transform: uppercase; color: var(--ink-faint); padding: 10px 14px; border-bottom: 1px solid var(--line-strong); white-space: nowrap; }
  thead th:first-child { text-align: left; }
  tbody td { text-align: right; font-family: var(--font-mono); padding: 10px 14px; border-bottom: 1px solid var(--line); white-space: nowrap; }
  tbody tr:last-child td { border-bottom: none; }
  tbody td:first-child { text-align: left; font-family: var(--font-display); }
  .violations-ok { color: var(--good); font-weight: 600; }
  .violations-bad { color: var(--bad); font-weight: 600; }

  .notes-section ol { padding-left: 1.3em; margin: 0; display: flex; flex-direction: column; gap: 10px; max-width: 72ch; }
  .notes-section li { padding-left: 4px; }
  .notes-section li::marker { font-family: var(--font-mono); color: var(--ink-faint); }

  footer { padding-top: 20px; border-top: 1px solid var(--line); font-family: var(--font-mono); font-size: 0.78rem; color: var(--ink-faint); display: flex; flex-wrap: wrap; gap: 6px 10px; align-items: center; }
</style>
</head>
<body>
<div class="page">

  <header class="masthead">
    <p class="eyebrow">messaging-lab &middot; benchmark report</p>
    <h1>Blip Rebalance Benchmark</h1>
    <p class="dek">
      $InstanceCount instance(s), one per partition, draining a $Count-message backlog &mdash; instance
      $BlipInstanceId deliberately disconnects and reconnects its own session ${BlipAfterSeconds}s in,
      staying down for ${BlipDurationSeconds}s, to reach <code>FlowEvent.FlowInactive</code> on an
      instance that was genuinely already active. $Trials trial(s).
    </p>
    <dl class="run-meta">
      <div><dt>Broker</dt><dd>$(Encode $AppSettings.Solace.Host) ($(Encode $AppSettings.Solace.VPNName))</dd></div>
      <div><dt>Route</dt><dd>$(Encode $Topic) &rarr; $(Encode $Queue)</dd></div>
      <div><dt>Partitions</dt><dd>$PartitionCount</dd></div>
      <div><dt>Instances</dt><dd>$InstanceCount (blip: #$BlipInstanceId)</dd></div>
      <div><dt>Concurrency / WindowSize</dt><dd>$Concurrency lane(s) &middot; $(if ($WindowSize -gt 0) { $WindowSize } else { "255 (default)" })</dd></div>
      <div><dt>Blip</dt><dd>after ${BlipAfterSeconds}s, lasts ${BlipDurationSeconds}s</dd></div>
      <div><dt>Load / run</dt><dd>$Count msgs &middot; $KeyCount keys</dd></div>
      <div><dt>Simulated work</dt><dd>$($AppSettings.Subscriber.SimulatedHandlerWorkMinMs)&ndash;$($AppSettings.Subscriber.SimulatedHandlerWorkMaxMs) ms</dd></div>
      <div><dt>Run date</dt><dd>$RunDate</dd></div>
    </dl>
  </header>

  <section class="headline-stats">
    <div class="stat-tile $(if ($totalGlobalDuplicates -eq 0) { 'status-good' } else { 'status-bad' })">
      <p class="stat-value tabular">$totalGlobalDuplicates</p>
      <p class="stat-label">global duplicate handling(s)<br>across $($Results.Count) trial(s)</p>
    </div>
    <div class="stat-tile status-good">
      <p class="stat-value tabular">$totalDropped</p>
      <p class="stat-label">buffered messages the blip instance<br>dropped instead of handling</p>
    </div>
    <div class="stat-tile $(if ($trialsWithoutInactive -eq 0) { 'status-good' } else { 'status-bad' })">
      <p class="stat-value tabular">$($Results.Count - $trialsWithoutInactive)/$($Results.Count)</p>
      <p class="stat-label">trial(s) where the blip instance<br>actually went inactive</p>
    </div>
  </section>

  <section class="table-section">
    <h2>What happened, per trial</h2>
    <p class="section-intro">$headlineNote</p>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Trial</th><th>Messages</th><th>Elapsed</th><th>Throughput</th>
            <th>Inactive/Active events</th><th>Dropped</th><th>Global dup.</th><th>Global reorder</th>
          </tr>
        </thead>
        <tbody>
$($rows -join "`n")
        </tbody>
      </table>
    </div>
  </section>

  <section class="notes-section">
    <h2>Method &amp; caveats</h2>
    <p class="chart-caption">Read the numbers above alongside these notes on how this benchmark was run.</p>
    <ol>
      <li><strong>Why this scenario needs a simulated blip at all.</strong> Adding more consumers than partitions never displaces an existing owner - only a genuine disconnect reaches FlowInactive on an instance that was truly already active. See the script header for the probes that established this.</li>
      <li><strong>"Inactive/Active events" should read "1 / 1" per trial.</strong> One FlowInactive when the blip starts, one FlowActive once the broker re-admits the reconnected instance (typically ~5s - the queue's partitionRebalanceDelay - after BlipDurationSeconds elapses). If a trial shows 0/0, the blip didn't land as designed for that trial; its other numbers are inconclusive.</li>
      <li><strong>"Dropped" is the fix engaging, not a bug.</strong> It counts messages the blip instance discarded (never called the handler on) because they were buffered before its last FlowInactive - see SolaceConcurrentSubscriber&lt;T&gt;.LogDropped. Zero here alongside real backlog would mean the blip fired too late to catch anything in flight, not that there was nothing to catch.</li>
      <li><strong>Some residual duplication is possible even with Dropped > 0.</strong> The generation check only stops a message from being handled if it hasn't started yet. A message already inside a synchronous Handle() call at the exact moment of disconnect finishes normally and acks (or tries to) regardless - that one specific race isn't closed by this fix, only the much larger population of still-queued backlog is.</li>
      <li><strong>One instance per partition is deliberate.</strong> It makes the blip instance's partition loss unambiguous (it had exactly one, now it has zero) rather than one of several, which is what makes this scenario cleanly attributable to it specifically.</li>
      <li><strong>Every instance is torn down with Stop-Job (a hard kill) at the end of each trial.</strong> Only the blip in the middle of a trial is a deliberate, controlled disconnect; the final teardown is the same abrupt stop the other multi-subscriber benchmarks use.</li>
    </ol>
  </section>

  <footer>
    <span>messaging-lab.solace.loadgen</span>
    <span>&rarr;</span>
    <span>$(Encode $Topic)</span>
    <span>&rarr;</span>
    <span>$(Encode $Queue) ($PartitionCount partitions)</span>
    <span>&rarr;</span>
    <span>$InstanceCount &times; messaging-lab.solace.subscriber (instance $BlipInstanceId blips)</span>
    <span>&middot;</span>
    <span>$Count msgs &times; $($Results.Count) trial(s) &middot; $KeyCount keys/run</span>
  </footer>

</div>
</body>
</html>
"@

$html | Set-Content -Path $ReportPath -Encoding utf8

Write-Host ""
Write-Host "Report written to $ReportPath"
