<#
.SYNOPSIS
    Exercises a partition handoff *while a consumer still has unacked backlog*, rather than at
    process startup: starts a few subscriber instances on a partitioned queue, lets them get partway
    through a slow batch (so they genuinely hold delivered-but-unacked messages), then binds more
    instances mid-drain to force the broker to rebalance partitions away from the busy ones. Sweeps
    -Subscriber:WindowSize to show its effect on how many messages get caught in the resulting
    "disturbed sequence" redelivery. Writes a self-contained HTML report to reports/.

.DESCRIPTION
    run-benchmark-multisubscriber.ps1 starts every instance before publishing, so partitions are
    assigned once, up front, and no rebalance happens mid-drain - it measures steady-state
    competing-consumer throughput, not handoff behavior. This script does the opposite on purpose:
    publish first, start a small number of instances, wait -JoinDelaySeconds (deliberately short
    relative to the slow simulated handler work below), then bind -JoiningInstanceCount more. That
    bind is exactly one of Solace's rebalance triggers (see Partition-Rebalancing.htm), and with
    real backlog still sitting unacked on the instances that were already running, some of it is
    likely to still be outstanding when partitionRebalanceMaxHandoffTime expires - which is when
    Solace's docs say a "disturbed" sequence gets resent whole to whichever consumer now owns the
    partition (see Partition-Handoff.htm), regardless of whether the original consumer already
    started (or even finished, but hadn't acked) handling part of it.

    Two duplicate/violation counts are reported per configuration, and they measure different
    things:
      - "Per-process" is each instance's own OrderingValidator (see OrderHandler.cs) - it can only
        ever see a duplicate if the *same* process re-handles a sequence it already handled itself.
      - "Global" merges every instance's structured "HANDLED OrderId=... Sequence=..." log line by
        timestamp across ALL instances in the run (the same technique
        run-benchmark-multisubscriber-nonpartitioned.ps1 uses), which is the only way to see the
        handoff case that actually matters here: the original (slow) consumer handles a message,
        and the new owner the broker handed the partition to handles the same message again. Expect
        "Global" duplicates to be the number that moves with -WindowSizes; "Per-process" duplicates
        should stay near zero throughout, since it's the wrong lens for this specific failure mode.

    What this does NOT cover: Solace's FlowActive/FlowInactive events (now wired up in
    SolaceConcurrentSubscriber<T>) only fire when a flow goes to/from *zero* partitions, but adding
    instances here causes each existing instance to give up *some* partitions while keeping others -
    a partial loss, which never triggers FlowInactive (see CLAUDE.md / the session's own notes on
    this). So this sweep validates the -WindowSize lever, not the FlowActive/Inactive pause path;
    exercising that would need a topology where an already-active instance is driven to exactly zero
    partitions (e.g. InstanceCount == PartitionCount, then scale down), which is a different script.
    Note also that every instance here is torn down with Stop-Job (a hard kill, same as the other
    multi-subscriber benchmarks) rather than a graceful stop, so the drain-before-disconnect fix in
    SubscriberHostedService is likewise not exercised by this sweep either - only the join path is.

.PARAMETER PartitionCount
    The partition count the target queue (-Queue) was created with. Purely descriptive here (unlike
    run-benchmark-multisubscriber.ps1, nothing in this script scales with it) - shown in the report.

.PARAMETER WindowSizes
    Values passed as -Subscriber:WindowSize to every instance in a configuration, one configuration
    per value. 0 means "omit the flag" (leaves the SDK's own default of 255 in place). Default
    0,16,4 - the default alongside two shrinking values, to show whether a smaller flow-control
    window measurably reduces how much backlog gets caught in a disturbed handoff.

.PARAMETER InitialInstanceCount
    Subscriber instances started first and given -JoinDelaySeconds to bind and start accumulating
    real unacked backlog before anything else joins.

.PARAMETER JoiningInstanceCount
    Additional subscriber instances started -JoinDelaySeconds after the initial ones, bound to the
    same queue - this bind is what triggers the mid-drain rebalance.

.PARAMETER Concurrency
    Per-instance lane count (SolaceConcurrentSubscriber<T> concurrency), held fixed across the sweep
    so -WindowSizes is the only thing that varies between configurations.

.PARAMETER JoinDelaySeconds
    Seconds between starting the initial instances and starting the joining ones. Deliberately short
    relative to the batch size and simulated handler cost below, so there's still substantial
    backlog outstanding on the initial instances when the rebalance actually reassigns partitions
    (rebalance delay + max handoff time later - see the queue's own partitionRebalanceDelay /
    partitionRebalanceMaxHandoffTime, reported via SEMP).

.PARAMETER Count
    Messages published per configuration. Higher than run-benchmark-multisubscriber.ps1's default
    on purpose - there needs to be enough backlog left that JoinDelaySeconds doesn't let the initial
    instances finish before the joiners even bind.

.PARAMETER KeyCount
    Distinct OrderId keys per configuration, round-robin.

.PARAMETER Queue / Topic
    The partitioned queue and the topic it's subscribed to. Defaults match
    run-benchmark-multisubscriber.ps1's CHANGED-P / data-changed-p.

.PARAMETER DrainTimeoutSeconds
    Max seconds to wait for a configuration's backlog to fully drain before giving up on it. Defaults
    (0) to a value scaled from Count and the subscriber's SimulatedHandlerWorkMaxMs, as in the other
    multi-subscriber benchmarks.

.PARAMETER SempBaseUrl / SempUser / SempPassword
    Broker admin SEMP endpoint used to poll queue depth. Defaults match the local Docker broker
    documented in scripts/start-solace.ps1.

.EXAMPLE
    ./scripts/run-benchmark-mid-drain-rebalance.ps1 -PartitionCount 8

.EXAMPLE
    ./scripts/run-benchmark-mid-drain-rebalance.ps1 -PartitionCount 8 -WindowSizes 0,32,8,4 -Count 800
#>

param(
    [int]$PartitionCount = 8,
    [int[]]$WindowSizes = @(0, 16, 4),
    [int]$InitialInstanceCount = 2,
    [int]$JoiningInstanceCount = 2,
    [int]$Concurrency = 4,
    [int]$JoinDelaySeconds = 4,
    [int]$Count = 400,
    [int]$KeyCount = 64,
    [string]$Queue = "CHANGED-P",
    [string]$Topic = "data-changed-p",
    [int]$DrainTimeoutSeconds = 0,
    [string]$SempBaseUrl = "http://localhost:8080",
    [string]$SempUser = "admin",
    [string]$SempPassword = "admin"
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$SubscriberDir = Join-Path $RepoRoot "messaging-lab.solace.subscriber"
$LoadgenDir = Join-Path $RepoRoot "messaging-lab.solace.loadgen"
$LogsDir = Join-Path $SubscriberDir "bin/Debug/net10.0/logs"

$AppSettings = Get-Content (Join-Path $SubscriberDir "appsettings.json") -Raw | ConvertFrom-Json
$MetricsReportIntervalSeconds = [int]$AppSettings.Subscriber.MetricsReportIntervalSeconds
$TotalInstanceCount = $InitialInstanceCount + $JoiningInstanceCount

if ($DrainTimeoutSeconds -le 0) {
    $worstCaseSeconds = $Count * $AppSettings.Subscriber.SimulatedHandlerWorkMaxMs / 1000
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

function Format-WindowSize([int]$WindowSize) {
    if ($WindowSize -le 0) { return "255 (default)" }
    return "$WindowSize"
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
if ($JoiningInstanceCount -le 0) {
    Write-Error "-JoiningInstanceCount must be at least 1 - with 0, nothing ever joins mid-drain and there's no rebalance to measure."
    exit 1
}

Write-Host "Sweeping $($WindowSizes.Count) WindowSize value(s): $(($WindowSizes | ForEach-Object { Format-WindowSize $_ }) -join ', ')"
Write-Host "$InitialInstanceCount initial instance(s), +$JoiningInstanceCount joining after ${JoinDelaySeconds}s, $Concurrency lane(s)/instance, PartitionCount=$PartitionCount"

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

function Start-SubscriberJob([int]$InstanceId, [int]$WindowSize) {
    Start-Job -ScriptBlock {
        param($subscriberDir, $dll, $queue, $concurrency, $instanceId, $windowSize)
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
        dotnet $dll @cliArgs
    } -ArgumentList $SubscriberDir, $SubscriberDll, $Queue, $Concurrency, $InstanceId, $WindowSize
}

$MetricsPattern = 'Handled (\d+) messages in ([\d:.]+) \(([\d.]+) msgs/sec\) - ordering violations: (\d+), duplicates: (\d+), latency p50=([\d.]+)ms p99=([\d.]+)ms'
$HandledPattern = '^(?<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [+\-]\d{2}:\d{2})\s+\[INF\]\s+\[[^\]]*\][^:]*:\s*HANDLED OrderId=(?<oid>\S+) Sequence=(?<seq>\d+)'

# Same technique as run-benchmark-multisubscriber-nonpartitioned.ps1's Measure-GlobalOrderingViolations:
# each instance's own OrderingValidator can only see duplicates/reorders *it* personally handled twice,
# so a message the original (slow) owner handled once and the post-handoff new owner handled again is
# invisible to either instance's own counters. Merging every instance's structured HANDLED log line by
# timestamp, across process boundaries, is the only way to see that.
function Get-HandledEvents {
    param([int]$InstanceId, [datetime]$Start, [datetime]$End)

    # -Filter's glob matches every rolled-over date for this instance (e.g. both
    # subscriber-1-20260920.log and subscriber-1-20260923.log), and Get-ChildItem doesn't sort by
    # date - picking the wrong one silently returns zero events for the actual run instead of an
    # error. Sort by LastWriteTime so today's (still being appended to) file always wins.
    $file = Get-ChildItem -Path $LogsDir -Filter "subscriber-$InstanceId-*.log" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
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

# Publishes a batch, starts $InitialInstanceCount instances, waits $JoinDelaySeconds, starts
# $JoiningInstanceCount more against the same queue (the bind that triggers the rebalance), waits for
# the broker to report the queue fully drained, then stops everything and returns both the per-instance
# and the merged global ordering/duplicate counts for this configuration.
function Invoke-MidDrainRun {
    param([int]$WindowSize)

    Publish-Batch
    $configStart = Get-Date

    $jobs = 1..$InitialInstanceCount | ForEach-Object { Start-SubscriberJob -InstanceId $_ -WindowSize $WindowSize }

    Write-Host "  Initial $InitialInstanceCount instance(s) started; waiting ${JoinDelaySeconds}s before joining $JoiningInstanceCount more..."
    Start-Sleep -Seconds $JoinDelaySeconds

    $joiningIds = ($InitialInstanceCount + 1)..$TotalInstanceCount
    $jobs += $joiningIds | ForEach-Object { Start-SubscriberJob -InstanceId $_ -WindowSize $WindowSize }
    Write-Host "  Joined instance(s) $($joiningIds -join ', ') mid-drain."

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
        throw "Timed out after ${DrainTimeoutSeconds}s waiting for WindowSize=$(Format-WindowSize $WindowSize) to drain (queue usage=$(Get-QueueSpoolUsage)). Aborting the sweep - increase -DrainTimeoutSeconds and re-run."
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

    $global = Measure-GlobalOrdering -InstanceCount $TotalInstanceCount -Start $configStart -End $configEnd

    [pscustomobject]@{
        ElapsedS           = $drainStopwatch.Elapsed.TotalSeconds
        PerInstance        = @($perInstance)
        GlobalEventCount   = $global.EventCount
        GlobalDuplicates   = $global.Duplicates
        GlobalOutOfOrder   = $global.OutOfOrder
    }
}

# --- Run the sweep --------------------------------------------------------------

$Results = @()

foreach ($ws in $WindowSizes) {
    Write-Host "`n=== WindowSize=$(Format-WindowSize $ws): publishing $Count messages across $KeyCount keys ==="
    $run = Invoke-MidDrainRun -WindowSize $ws

    $totalMessages = ($run.PerInstance | Measure-Object -Property Messages -Sum).Sum
    $totalPerProcessViolations = ($run.PerInstance | Measure-Object -Property Violations -Sum).Sum
    $totalPerProcessDuplicates = ($run.PerInstance | Measure-Object -Property Duplicates -Sum).Sum

    if ($totalMessages -ne $Count) {
        Write-Warning "WindowSize=$(Format-WindowSize $ws) reported $totalMessages of $Count published messages handled across all instances - the snapshot may not reflect the full drain."
    }

    $Results += [pscustomobject]@{
        WindowSize               = $ws
        WindowSizeLabel          = Format-WindowSize $ws
        Messages                 = $totalMessages
        ElapsedS                 = $run.ElapsedS
        Rate                     = if ($run.ElapsedS -gt 0) { $totalMessages / $run.ElapsedS } else { 0.0 }
        PerProcessViolations     = $totalPerProcessViolations
        PerProcessDuplicates     = $totalPerProcessDuplicates
        GlobalEventCount         = $run.GlobalEventCount
        GlobalDuplicates         = $run.GlobalDuplicates
        GlobalOutOfOrder         = $run.GlobalOutOfOrder
    }

    Write-Host "  Messages=$totalMessages  GlobalDuplicates=$($run.GlobalDuplicates)  GlobalOutOfOrder=$($run.GlobalOutOfOrder)  PerProcessDuplicates=$totalPerProcessDuplicates"
}

if ($Results.Count -eq 0) {
    Write-Error "No configuration produced a result; nothing to report."
    exit 1
}

# --- Report: assemble HTML ------------------------------------------------------

function Encode([string]$s) { [System.Net.WebUtility]::HtmlEncode("$s") }

function Get-NiceTicks {
    param([double]$maxValue, [int]$targetCount = 5)
    if ($maxValue -le 0) { return [pscustomobject]@{ Ticks = @(0, 1); DomainMax = 1 } }

    $roughStep = $maxValue / $targetCount
    $magnitude = [math]::Pow(10, [math]::Floor([math]::Log10($roughStep)))
    $residual = $roughStep / $magnitude
    $niceResidual = if ($residual -lt 1.5) { 1 } elseif ($residual -lt 3) { 2 } elseif ($residual -lt 7) { 5 } else { 10 }
    $step = $niceResidual * $magnitude

    $domainMax = [math]::Ceiling($maxValue / $step) * $step
    $ticks = @()
    for ($v = 0; $v -le $domainMax + ($step * 0.001); $v += $step) { $ticks += [math]::Round($v, 6) }
    [pscustomobject]@{ Ticks = $ticks; DomainMax = $domainMax }
}

# One bar per WindowSize value, showing GlobalDuplicates - the metric this sweep exists to move.
function New-DuplicatesBarChartSvg {
    param($Results)

    $plotLeft = 46; $plotRight = 700; $baselineY = 260; $plotTop = 26
    $maxDup = ($Results | Measure-Object -Property GlobalDuplicates -Maximum).Maximum
    $ticks = Get-NiceTicks -maxValue ([math]::Max($maxDup * 1.15, 1))
    $yScale = ($baselineY - $plotTop) / $ticks.DomainMax

    $barCount = $Results.Count
    $groupWidth = ($plotRight - $plotLeft) / $barCount
    $barWidth = [math]::Min(70, $groupWidth * 0.5)

    $svg = New-Object System.Collections.Generic.List[string]
    $svg.Add("<svg viewBox='0 0 720 300' xmlns='http://www.w3.org/2000/svg' role='img' aria-label='Bar chart of globally-detected duplicate message handling by WindowSize'>")
    $svg.Add("<text x='14' y='$($plotTop - 6)' class='axis-label'>duplicates</text>")

    foreach ($t in $ticks.Ticks) {
        $y = $baselineY - ($t * $yScale)
        $svg.Add("<line x1='$plotLeft' x2='$plotRight' y1='$y' y2='$y' class='grid-line' />")
        $svg.Add("<text x='$($plotLeft - 10)' y='$($y + 3)' text-anchor='end' class='axis-label'>$t</text>")
    }

    for ($i = 0; $i -lt $barCount; $i++) {
        $r = $Results[$i]
        $groupX = $plotLeft + $i * $groupWidth
        $barX = $groupX + ($groupWidth - $barWidth) / 2
        $barH = [math]::Max($r.GlobalDuplicates * $yScale, 1)
        $barY = $baselineY - $barH
        $color = if ($r.GlobalDuplicates -eq 0) { '#2f7d52' } else { '#a3341f' }

        $svg.Add("<rect x='$barX' y='$barY' width='$barWidth' height='$barH' rx='2' fill='$color' />")
        $svg.Add("<text x='$($groupX + $groupWidth / 2)' y='$($barY - 6)' text-anchor='middle' class='cat-label'>$($r.GlobalDuplicates)</text>")
        $svg.Add("<text x='$($groupX + $groupWidth / 2)' y='$($baselineY + 20)' text-anchor='middle' class='cat-label'>WindowSize=$(Encode $r.WindowSizeLabel)</text>")
    }

    $svg.Add("</svg>")
    return ($svg -join "`n")
}

$ReportsDir = Join-Path $RepoRoot "reports"
New-Item -ItemType Directory -Path $ReportsDir -Force | Out-Null
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$RunDate = Get-Date -Format "yyyy-MM-dd"
$ReportPath = Join-Path $ReportsDir "benchmark-mid-drain-rebalance-$Timestamp.html"

$totalGlobalDuplicates = ($Results | Measure-Object -Property GlobalDuplicates -Sum).Sum
$totalGlobalOutOfOrder = ($Results | Measure-Object -Property GlobalOutOfOrder -Sum).Sum
$totalMessagesAll = ($Results | Measure-Object -Property Messages -Sum).Sum

$rows = New-Object System.Collections.Generic.List[string]
foreach ($r in $Results) {
    $rows.Add(@"
          <tr>
            <td>$(Encode $r.WindowSizeLabel)</td>
            <td class="tabular">$($r.Messages)</td>
            <td class="tabular">$([math]::Round($r.ElapsedS, 2))s</td>
            <td class="tabular">$([math]::Round($r.Rate, 1))/s</td>
            <td class="tabular $(if ($r.GlobalDuplicates -eq 0) { 'violations-ok' } else { 'violations-bad' })">$($r.GlobalDuplicates)</td>
            <td class="tabular $(if ($r.GlobalOutOfOrder -eq 0) { 'violations-ok' } else { 'violations-bad' })">$($r.GlobalOutOfOrder)</td>
            <td class="tabular">$($r.PerProcessDuplicates)</td>
            <td class="tabular">$($r.PerProcessViolations)</td>
          </tr>
"@)
}

$duplicatesChart = New-DuplicatesBarChartSvg -Results $Results

$headlineNote = if ($totalGlobalDuplicates -eq 0) {
    "No duplicates observed at any WindowSize in this sweep - either JoinDelaySeconds/Count/simulated work weren't aggressive enough to leave real backlog outstanding at handoff time, or the handoff genuinely completed cleanly every time. Try a larger -Count, a shorter -JoinDelaySeconds, or slower SimulatedHandlerWork*Ms in appsettings.json to manufacture more backlog."
}
else {
    "$totalGlobalDuplicates duplicate handling(s) detected globally across $($Results.Count) configuration(s) ($totalMessagesAll messages) - i.e. Solace's documented 'disturbed sequence' resend actually fired: a message the original consumer already handled got handed to the new owner and handled again."
}

$html = @"
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mid-Drain Rebalance Benchmark - $Timestamp</title>
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
  .chart-frame { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 18px 18px 6px; }
  .chart-frame svg { display: block; width: 100%; height: auto; }

  .axis-label { fill: var(--ink-faint); font-family: var(--font-mono); font-size: 10.5px; }
  .grid-line { stroke: var(--line); stroke-width: 1; }
  .cat-label { fill: var(--ink); font-family: var(--font-display); font-weight: 600; font-size: 12.5px; }

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
    <h1>Mid-Drain Rebalance Benchmark</h1>
    <p class="dek">
      $InitialInstanceCount instance(s) start draining a $Count-message backlog, then $JoiningInstanceCount more
      bind ${JoinDelaySeconds}s later, forcing Solace to rebalance partitions on a $PartitionCount-partition queue
      while real unacked backlog is still outstanding &mdash; sweeping <code>Subscriber:WindowSize</code>
      across $($Results.Count) configuration(s).
    </p>
    <dl class="run-meta">
      <div><dt>Broker</dt><dd>$(Encode $AppSettings.Solace.Host) ($(Encode $AppSettings.Solace.VPNName))</dd></div>
      <div><dt>Route</dt><dd>$(Encode $Topic) &rarr; $(Encode $Queue)</dd></div>
      <div><dt>Partitions</dt><dd>$PartitionCount</dd></div>
      <div><dt>Instances</dt><dd>$InitialInstanceCount initial + $JoiningInstanceCount joining</dd></div>
      <div><dt>Join delay</dt><dd>${JoinDelaySeconds}s</dd></div>
      <div><dt>Load / run</dt><dd>$Count msgs &middot; $KeyCount keys</dd></div>
      <div><dt>Simulated work</dt><dd>$($AppSettings.Subscriber.SimulatedHandlerWorkMinMs)&ndash;$($AppSettings.Subscriber.SimulatedHandlerWorkMaxMs) ms</dd></div>
      <div><dt>Run date</dt><dd>$RunDate</dd></div>
    </dl>
  </header>

  <section class="headline-stats">
    <div class="stat-tile $(if ($totalGlobalDuplicates -eq 0) { 'status-good' } else { 'status-bad' })">
      <p class="stat-value tabular">$totalGlobalDuplicates</p>
      <p class="stat-label">duplicate handling(s), globally,<br>across $($Results.Count) configurations</p>
    </div>
    <div class="stat-tile $(if ($totalGlobalOutOfOrder -eq 0) { 'status-good' } else { 'status-bad' })">
      <p class="stat-value tabular">$totalGlobalOutOfOrder</p>
      <p class="stat-label">global out-of-order (non-duplicate)<br>handling(s)</p>
    </div>
    <div class="stat-tile">
      <p class="stat-value tabular">$totalMessagesAll</p>
      <p class="stat-label">total messages handled<br>across the sweep</p>
    </div>
  </section>

  <section class="chart-section">
    <h2>Global duplicates by WindowSize</h2>
    <p class="chart-caption">$headlineNote</p>
    <div class="chart-frame">
$duplicatesChart
    </div>
  </section>

  <section class="table-section">
    <h2>Full results</h2>
    <p class="section-intro">"Global" columns merge every instance's HANDLED log line by timestamp across process boundaries (see script header); "per-process" columns are each instance's own in-process OrderingValidator, summed. Per-process duplicates staying near zero while global duplicates move is the expected signature of a handoff-caused duplicate, not a bug in either counter.</p>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>WindowSize</th><th>Messages</th><th>Elapsed</th><th>Throughput</th>
            <th>Global dup.</th><th>Global reorder</th><th>Per-proc dup.</th><th>Per-proc reorder</th>
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
      <li><strong>One trial per WindowSize value.</strong> Rebalance timing races against real wall-clock delays (JoinDelaySeconds, the broker's own partitionRebalanceDelay/partitionRebalanceMaxHandoffTime), so a single run can land on either side of the timing window that produces a duplicate. Treat this as indicative, not a precise rate.</li>
      <li><strong>Global vs. per-process counts measure different things.</strong> See the table caption above - only the global, cross-process merge can see a message the original consumer handled once and the post-handoff new owner handled again.</li>
      <li><strong>This exercises the partial-rebalance path, not FlowActive/FlowInactive.</strong> Adding instances here causes each existing one to give up some (not all) of its partitions, which never fires Solace's FlowInactive event - see the script header for why, and what topology would be needed to exercise that path instead.</li>
      <li><strong>Every instance is torn down with Stop-Job (a hard kill), including at the end of each configuration.</strong> The graceful drain-before-disconnect fix in SubscriberHostedService is not exercised by this sweep - it only matters for a real, signaled shutdown, which doesn't happen here.</li>
      <li><strong>The simulated handler cost and JoinDelaySeconds are what make a handoff-time duplicate possible at all.</strong> If SimulatedHandlerWork*Ms in appsettings.json is fast enough (or JoinDelaySeconds long enough) that the initial instances fully drain before joiners even bind, there's nothing for the joiners to be handed off mid-flight and this sweep degenerates to run-benchmark-multisubscriber.ps1's startup-only case.</li>
    </ol>
  </section>

  <footer>
    <span>messaging-lab.solace.loadgen</span>
    <span>&rarr;</span>
    <span>$(Encode $Topic)</span>
    <span>&rarr;</span>
    <span>$(Encode $Queue) ($PartitionCount partitions)</span>
    <span>&rarr;</span>
    <span>$InitialInstanceCount + $JoiningInstanceCount &times; messaging-lab.solace.subscriber</span>
    <span>&middot;</span>
    <span>$Count msgs &times; $($Results.Count) configs &middot; $KeyCount keys/run</span>
  </footer>

</div>
</body>
</html>
"@

$html | Set-Content -Path $ReportPath -Encoding utf8

Write-Host ""
Write-Host "Report written to $ReportPath"
