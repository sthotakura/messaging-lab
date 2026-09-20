<#
.SYNOPSIS
    Runs a competing-consumers benchmark: sweeps subscriber process count x per-instance lane
    count against a partitioned queue, publishing a fresh batch per configuration and draining
    it through that many concurrently-running messaging-lab.solace.subscriber processes. Writes
    a self-contained HTML report to reports/.

.DESCRIPTION
    Unlike scripts/run-benchmark.ps1 (one subscriber process, N in-process worker lanes), this
    scales out horizontally: multiple OS processes bind to the same partitioned queue, and the
    broker itself hands each bound consumer a disjoint set of partitions. A message's partition
    key (its OrderId - see messaging-lab.orders.OrderKeySelector, applied on publish) is hashed
    to exactly one partition, and a partition is only ever owned by one consumer at a time, so
    same-key ordering holds globally across processes with no extra bookkeeping - each process's
    own OrderingValidator is already a valid global check, because a key can never be split
    across two processes.

.PARAMETER PartitionCount
    The partition count the target queue (-Queue) was created with. Required: it's a broker-side,
    creation-time property this script has no way to discover on its own, and it drives both the
    default instance-count sweep and the interpretation of the results (an instance count above
    this leaves the extra processes permanently idle - only PartitionCount consumers can ever be
    assigned a partition).

.PARAMETER InstanceCounts
    Subscriber process counts to sweep. Defaults to 1, 2, 4, PartitionCount/2, PartitionCount,
    PartitionCount*2 (deduplicated, sorted) - the last point deliberately exceeds PartitionCount
    to show the idle-consumer ceiling directly.

.PARAMETER Concurrencies
    Per-instance lane count to sweep (each instance binds SolaceConcurrentSubscriber<T> with this
    many worker lanes). Default 1,2,4,8. Every (InstanceCount, Concurrency) pair is run.

.PARAMETER Count
    Messages published per configuration.

.PARAMETER KeyCount
    Distinct OrderId keys per configuration, round-robin. Kept comfortably above PartitionCount
    by default so partition-to-consumer load balance isn't dominated by hash luck on a handful
    of keys (the same lesson the lane-count benchmark hit at KeyCount=16/n=32).

.PARAMETER Queue / Topic
    The partitioned queue and the topic it's subscribed to. Defaults assume a queue named
    CHANGED-P (non-exclusive, PartitionCount partitions) subscribed to topic data-changed-p -
    kept separate from the CHANGED/data-changed pair the single-subscriber benchmark uses, so
    that benchmark's historical results and queue access-type aren't disturbed.

.PARAMETER DrainTimeoutSeconds
    Max seconds to wait for a single configuration's backlog to fully drain before giving up on it.
    Defaults (0) to a value scaled from Count and the subscriber's SimulatedHandlerWorkMaxMs, sized
    for the slowest configuration in the sweep (InstanceCount=1, Concurrency=1 - fully sequential),
    plus a safety margin. A timeout here aborts the whole sweep rather than continuing - see the
    note on the per-configuration drain check below.

.PARAMETER SempBaseUrl / SempUser / SempPassword
    Broker admin SEMP endpoint used to poll queue depth. Defaults match the local Docker broker
    documented in scripts/start-solace.ps1.

.EXAMPLE
    ./scripts/run-benchmark-multisubscriber.ps1 -PartitionCount 8

.EXAMPLE
    ./scripts/run-benchmark-multisubscriber.ps1 -PartitionCount 8 -InstanceCounts 1,4,8,16 -Concurrencies 1,4
#>

param(
    [Parameter(Mandatory)] [int]$PartitionCount,
    [int[]]$InstanceCounts,
    [int[]]$Concurrencies = @(1, 2, 4, 8),
    [int]$Count = 100,
    [int]$KeyCount = 64,
    [string]$Queue = "CHANGED-P",
    [string]$Topic = "data-changed-p",
    [int]$DrainTimeoutSeconds = 0,
    [string]$SempBaseUrl = "http://localhost:8080",
    [string]$SempUser = "admin",
    [string]$SempPassword = "admin"
)

$ErrorActionPreference = "Stop"

if (-not $InstanceCounts) {
    # Parenthesize every element explicitly - PowerShell's comma operator binds tighter than `*`
    # here, so an unparenthesized trailing `$PartitionCount * 2` multiplies the whole preceding
    # comma list (array replication), not just that last term.
    $InstanceCounts = @(1, 2, 4, [Math]::Max(1, [int]($PartitionCount / 2)), $PartitionCount, ($PartitionCount * 2)) |
        Sort-Object -Unique
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$SubscriberDir = Join-Path $RepoRoot "messaging-lab.solace.subscriber"
$LoadgenDir = Join-Path $RepoRoot "messaging-lab.solace.loadgen"

$AppSettings = Get-Content (Join-Path $SubscriberDir "appsettings.json") -Raw | ConvertFrom-Json
$MetricsReportIntervalSeconds = [int]$AppSettings.Subscriber.MetricsReportIntervalSeconds

if ($DrainTimeoutSeconds -le 0) {
    # Worst case in the sweep is InstanceCount=1/Concurrency=1 (no parallelism at all): Count
    # messages fully sequential at SimulatedHandlerWorkMaxMs each, plus a 50% safety margin.
    $worstCaseSeconds = $Count * $AppSettings.Subscriber.SimulatedHandlerWorkMaxMs / 1000
    $DrainTimeoutSeconds = [Math]::Max(300, [int]($worstCaseSeconds * 1.5))
    Write-Host "DrainTimeoutSeconds not specified - defaulting to ${DrainTimeoutSeconds}s (scaled for Count=$Count at the sweep's lowest parallelism)."
}

$AuthHeader = @{
    Authorization = "Basic " + [Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("${SempUser}:${SempPassword}"))
}
$QueueMonitorUrl = "$SempBaseUrl/SEMP/v2/monitor/msgVpns/default/queues/$Queue"

function Get-QueueSpoolUsage {
    (Invoke-RestMethod -Uri $QueueMonitorUrl -Headers $AuthHeader).data.msgSpoolUsage
}

# --- Preconditions ----------------------------------------------------------

try {
    $usage = Get-QueueSpoolUsage
}
catch {
    Write-Error "Could not reach queue '$Queue' via SEMP at $QueueMonitorUrl. Is the broker running (./scripts/start-solace.ps1), and does '$Queue' exist as a non-exclusive, $PartitionCount-partition queue subscribed to topic '$Topic'? See README > Comparing multiple subscribers."
    exit 1
}
if ($usage -ne 0) {
    Write-Error "Queue '$Queue' is not empty (msgSpoolUsage=$usage). Drain it before benchmarking so results aren't mixed with leftover messages."
    exit 1
}

$totalRuns = $InstanceCounts.Count * $Concurrencies.Count
Write-Host "Sweeping $($InstanceCounts.Count) instance count(s) x $($Concurrencies.Count) concurrency level(s) = $totalRuns configuration(s)."
Write-Host "Instance counts: $($InstanceCounts -join ', ')  |  Concurrencies: $($Concurrencies -join ', ')  |  PartitionCount: $PartitionCount"

# --- Build once, up front -----------------------------------------------------
# Concurrently launching `dotnet run` N times against the same project races on the same
# MSBuild output; build once here and exec the built DLL directly for every instance instead.

Write-Host "`n=== Building loadgen and subscriber (Debug) ==="
dotnet build (Join-Path $LoadgenDir "messaging-lab.solace.loadgen.csproj") -c Debug | Out-Null
dotnet build (Join-Path $SubscriberDir "messaging-lab.solace.subscriber.csproj") -c Debug | Out-Null

$LoadgenDll = Join-Path $LoadgenDir "bin/Debug/net10.0/messaging-lab.solace.loadgen.dll"
$SubscriberDll = Join-Path $SubscriberDir "bin/Debug/net10.0/messaging-lab.solace.subscriber.dll"
if (-not (Test-Path $LoadgenDll)) { Write-Error "Expected build output not found: $LoadgenDll"; exit 1 }
if (-not (Test-Path $SubscriberDll)) { Write-Error "Expected build output not found: $SubscriberDll"; exit 1 }

# --- Helpers -----------------------------------------------------------------

function Publish-Batch {
    Push-Location $LoadgenDir
    try {
        dotnet $LoadgenDll --LoadGen:Topic $Topic --LoadGen:Count $Count --LoadGen:KeyCount $KeyCount | Out-Null
    }
    finally {
        Pop-Location
    }
}

$MetricsPattern = 'Handled (\d+) messages in ([\d:.]+) \(([\d.]+) msgs/sec\) - ordering violations: (\d+), latency p50=([\d.]+)ms p99=([\d.]+)ms'

# Starts $InstanceCount subscriber processes bound to the partitioned queue, waits for the
# broker to report the queue fully drained, then stops them and parses each one's last metrics
# line. Wall-clock drain time is measured directly (start-of-consumption to SEMP-reports-zero)
# rather than trusting any single instance's own elapsed figure, since with N processes running
# concurrently no single instance's first-to-last-handled span necessarily covers the whole run.
function Invoke-MultiSubscriberRun {
    param([int]$InstanceCount, [int]$Concurrency)

    $jobs = 1..$InstanceCount | ForEach-Object {
        $instanceId = $_
        Start-Job -ScriptBlock {
            param($subscriberDir, $dll, $queue, $concurrency, $instanceId)
            Set-Location $subscriberDir
            dotnet $dll `
                --Subscriber:Queue $queue `
                --Subscriber:UseConcurrentSubscriber true `
                --Subscriber:Concurrency $concurrency `
                --Subscriber:InstanceId $instanceId
        } -ArgumentList $SubscriberDir, $SubscriberDll, $Queue, $Concurrency, $instanceId
    }

    $drainStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $waited = 0
    while ($waited -lt $DrainTimeoutSeconds) {
        Start-Sleep -Seconds 1
        $waited++
        if ((Get-QueueSpoolUsage) -eq 0) { break }
    }
    $drainStopwatch.Stop()
    if ($waited -ge $DrainTimeoutSeconds) {
        # Do NOT continue to the next configuration on a timeout: its subscriber processes are
        # still bound and mid-drain, so stopping them now leaves an unknown, non-zero backlog on
        # the queue. Publishing the next configuration's batch on top of that would silently mix
        # two configurations' messages together - exactly what happened before this check existed
        # (see README > Comparing multiple subscribers). Fail loudly instead.
        $jobs | Stop-Job | Out-Null
        $jobs | Remove-Job -Force
        throw "Timed out after ${DrainTimeoutSeconds}s waiting for InstanceCount=$InstanceCount/Concurrency=$Concurrency to drain (queue usage=$(Get-QueueSpoolUsage)). Aborting the sweep rather than risk mixing this backlog into the next configuration - increase -DrainTimeoutSeconds and re-run."
    }

    # Give each instance's periodic metrics reporter at least one full interval to log a snapshot
    # reflecting the drain above - Stop-Job kills processes without a graceful shutdown, so the
    # final Report() in MetricsReportingService's finally block never runs for any of them.
    Start-Sleep -Seconds ($MetricsReportIntervalSeconds + 1)
    $jobs | Stop-Job | Out-Null

    $perInstance = foreach ($job in $jobs) {
        $output = Receive-Job -Job $job
        $match = $output | Select-String -Pattern $MetricsPattern | Select-Object -Last 1
        if ($match) {
            $g = $match.Matches[0].Groups
            [pscustomobject]@{
                Messages   = [int]$g[1].Value
                Rate       = [double]$g[3].Value
                Violations = [int]$g[4].Value
                P50Ms      = [double]$g[5].Value
                P99Ms      = [double]$g[6].Value
            }
        }
        else {
            [pscustomobject]@{ Messages = 0; Rate = 0.0; Violations = 0; P50Ms = 0.0; P99Ms = 0.0 }
        }
    }
    $jobs | Remove-Job -Force

    [pscustomobject]@{
        ElapsedS    = $drainStopwatch.Elapsed.TotalSeconds
        PerInstance = @($perInstance)
    }
}

function Add-Result {
    param([int]$InstanceCount, [int]$Concurrency, $Run)

    $totalMessages = ($Run.PerInstance | Measure-Object -Property Messages -Sum).Sum
    $totalViolations = ($Run.PerInstance | Measure-Object -Property Violations -Sum).Sum
    $busy = @($Run.PerInstance | Where-Object { $_.Messages -gt 0 })
    $busyCount = $busy.Count
    $minP50 = if ($busyCount -gt 0) { ($busy | Measure-Object -Property P50Ms -Minimum).Minimum } else { 0.0 }
    $maxP99 = if ($busyCount -gt 0) { ($busy | Measure-Object -Property P99Ms -Maximum).Maximum } else { 0.0 }

    if ($totalMessages -ne $Count) {
        Write-Warning "Instances=$InstanceCount Concurrency=$Concurrency reported $totalMessages of $Count published messages handled across all instances - the snapshot may not reflect the full drain."
    }

    $script:Results += [pscustomobject]@{
        Instances     = $InstanceCount
        Concurrency   = $Concurrency
        BusyInstances = $busyCount
        Messages      = $totalMessages
        ElapsedS      = $Run.ElapsedS
        Rate          = if ($Run.ElapsedS -gt 0) { $totalMessages / $Run.ElapsedS } else { 0.0 }
        Violations    = $totalViolations
        MinP50Ms      = $minP50
        MaxP99Ms      = $maxP99
    }
}

# --- Run the matrix ----------------------------------------------------------

$Results = @()

foreach ($n in $InstanceCounts) {
    foreach ($c in $Concurrencies) {
        Write-Host "`n=== Instances=$n, Concurrency=$c/instance: publishing $Count messages across $KeyCount keys ==="
        Publish-Batch
        Write-Host "=== Instances=$n, Concurrency=$c/instance: draining across $n process(es) ==="
        $run = Invoke-MultiSubscriberRun -InstanceCount $n -Concurrency $c
        Add-Result -InstanceCount $n -Concurrency $c -Run $run
    }
}

if ($Results.Count -eq 0) {
    Write-Error "No configuration produced a result; nothing to report."
    exit 1
}

# --- Report: shared helpers ---------------------------------------------------

function Encode([string]$s) { [System.Net.WebUtility]::HtmlEncode("$s") }

# One color per concurrency level, consistent across every instance-count group in the chart.
$ConcurrencyPalette = @{}
$Palette = @('#8b93a0', '#d68a3f', '#bf7228', '#a35a16', '#7a3f0a', '#c96f22', '#8a4a12', '#5c2b06')
$paletteIdx = 0
foreach ($c in $Concurrencies) {
    $ConcurrencyPalette[$c] = $Palette[$paletteIdx % $Palette.Count]
    $paletteIdx++
}

# Rounds a rough axis step up to a "nice" 1/2/5 * 10^n value, then returns tick values from 0 up
# to the smallest such multiple that still covers $maxValue.
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

# Grouped bar chart: one group per instance count, one bar per concurrency level within it.
function New-GroupedThroughputChartSvg {
    param($Results, $InstanceCounts, $Concurrencies)

    $plotLeft = 46; $plotRight = 700; $baselineY = 280; $plotTop = 26
    $maxRate = ($Results | Measure-Object -Property Rate -Maximum).Maximum
    $ticks = Get-NiceTicks -maxValue ($maxRate * 1.15)
    $yScale = ($baselineY - $plotTop) / $ticks.DomainMax

    $groupCount = $InstanceCounts.Count
    $groupWidth = ($plotRight - $plotLeft) / $groupCount
    $barGap = 3
    $barWidth = [math]::Min(22, ($groupWidth - $barGap * ($Concurrencies.Count + 1)) / $Concurrencies.Count)

    $svg = New-Object System.Collections.Generic.List[string]
    $svg.Add("<svg viewBox='0 0 720 344' xmlns='http://www.w3.org/2000/svg' role='img' aria-label='Grouped bar chart of throughput in messages per second by instance count and concurrency'>")
    $svg.Add("<text x='14' y='$($plotTop - 6)' class='axis-label'>msgs/sec</text>")

    foreach ($t in $ticks.Ticks) {
        $y = $baselineY - ($t * $yScale)
        $svg.Add("<line x1='$plotLeft' x2='$plotRight' y1='$y' y2='$y' class='grid-line' />")
        $svg.Add("<text x='$($plotLeft - 10)' y='$($y + 3)' text-anchor='end' class='axis-label'>$t</text>")
    }

    for ($gi = 0; $gi -lt $groupCount; $gi++) {
        $n = $InstanceCounts[$gi]
        $groupX = $plotLeft + $gi * $groupWidth
        $barsWidth = $Concurrencies.Count * $barWidth + ($Concurrencies.Count - 1) * $barGap
        $barsStartX = $groupX + ($groupWidth - $barsWidth) / 2

        for ($ci = 0; $ci -lt $Concurrencies.Count; $ci++) {
            $c = $Concurrencies[$ci]
            $r = $Results | Where-Object { $_.Instances -eq $n -and $_.Concurrency -eq $c } | Select-Object -First 1
            if (-not $r) { continue }

            $barX = $barsStartX + $ci * ($barWidth + $barGap)
            $barH = [math]::Max($r.Rate * $yScale, 1)
            $barY = $baselineY - $barH
            $color = $ConcurrencyPalette[$c]

            $svg.Add("<rect x='$barX' y='$barY' width='$barWidth' height='$barH' rx='2' fill='$color' />")
        }

        $svg.Add("<text x='$($groupX + $groupWidth / 2)' y='$($baselineY + 20)' text-anchor='middle' class='cat-label'>$n inst.</text>")
    }

    $svg.Add("</svg>")
    return ($svg -join "`n")
}

# --- Report: compute headline figures -----------------------------------------

$fastest = $Results | Sort-Object Rate -Descending | Select-Object -First 1
$slowest = $Results | Sort-Object Rate | Select-Object -First 1
$fastestSpeedup = if ($slowest.Rate -gt 0) { [math]::Round($fastest.Rate / $slowest.Rate, 2) } else { 0 }
$totalMessages = ($Results | Measure-Object -Property Messages -Sum).Sum
$totalViolations = ($Results | Measure-Object -Property Violations -Sum).Sum

$saturated = $Results | Where-Object { $_.Instances -gt $PartitionCount -and $_.BusyInstances -le $PartitionCount }
$saturationNote = if ($saturated.Count -gt 0) {
    "Every configuration with more instances than the queue's $PartitionCount partitions shows only $PartitionCount (or fewer) busy instances - the rest bound successfully but were never assigned a partition, confirming the ceiling directly."
}
else {
    "No configuration in this sweep exceeded PartitionCount=$PartitionCount instances, so the idle-consumer ceiling wasn't exercised - rerun with a higher instance count to see it."
}

# --- Report: assemble HTML -----------------------------------------------------

$ReportsDir = Join-Path $RepoRoot "reports"
New-Item -ItemType Directory -Path $ReportsDir -Force | Out-Null
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$RunDate = Get-Date -Format "yyyy-MM-dd"
$ReportPath = Join-Path $ReportsDir "benchmark-multisubscriber-$Timestamp.html"

$rows = New-Object System.Collections.Generic.List[string]
foreach ($r in ($Results | Sort-Object Instances, Concurrency)) {
    $color = $ConcurrencyPalette[$r.Concurrency]
    $idleFlag = if ($r.Instances -gt $r.BusyInstances) { " <span class=`"idle-flag`">($($r.Instances - $r.BusyInstances) idle)</span>" } else { "" }
    $rows.Add(@"
          <tr>
            <td><span class="swatch" style="background:$color"></span>$($r.Instances)</td>
            <td class="tabular">$($r.Concurrency)</td>
            <td class="tabular">$($r.Instances * $r.Concurrency)</td>
            <td class="tabular">$($r.BusyInstances)$idleFlag</td>
            <td class="tabular">$($r.Messages)</td>
            <td class="tabular">$([math]::Round($r.ElapsedS, 2))s</td>
            <td class="tabular">$([math]::Round($r.Rate, 1))/s</td>
            <td class="tabular $(if ($r.Violations -eq 0) { 'violations-ok' } else { 'violations-bad' })">$($r.Violations)</td>
            <td class="tabular">$($r.MinP50Ms) ms</td>
            <td class="tabular">$($r.MaxP99Ms) ms</td>
          </tr>
"@)
}

$legendItems = New-Object System.Collections.Generic.List[string]
foreach ($c in $Concurrencies) {
    $legendItems.Add("<span class=`"item`"><span class=`"swatch`" style=`"background:$($ConcurrencyPalette[$c])`"></span>Concurrency=$c lane(s)/instance</span>")
}

$throughputChart = New-GroupedThroughputChartSvg -Results $Results -InstanceCounts $InstanceCounts -Concurrencies $Concurrencies

$violationsLine = if ($totalViolations -eq 0) {
    "Zero ordering violations across all $($Results.Count) configurations ($totalMessages messages) - the partition key (OrderId) kept every key's messages inside one partition, and therefore one process, for the whole sweep."
}
else {
    "$totalViolations ordering violation(s) were observed across $totalMessages messages handled - investigate before trusting partition-key routing on this run (a mid-run rebalance from a slow-starting instance is the most likely cause)."
}

$html = @"
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Multi-Subscriber Benchmark - $Timestamp</title>
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
  .legend { display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 12px; font-family: var(--font-mono); font-size: 0.76rem; color: var(--ink-soft); }
  .legend .item { display: inline-flex; align-items: center; gap: 6px; }
  .swatch { width: 10px; height: 10px; border-radius: 3px; display: inline-block; flex: none; margin-right: 6px; vertical-align: middle; }
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
  .idle-flag { color: var(--ink-faint); font-size: 0.82em; }

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
    <h1>Multi-Subscriber Benchmark</h1>
    <p class="dek">
      $($Results.Count) configuration$(if ($Results.Count -ne 1) { "s" }) of a $Count-message backlog, competing consumers on a
      $PartitionCount-partition queue &mdash; sweeping subscriber process count and per-instance
      <code>SolaceConcurrentSubscriber&lt;T&gt;</code> lane count together.
    </p>
    <dl class="run-meta">
      <div><dt>Broker</dt><dd>$(Encode $AppSettings.Solace.Host) ($(Encode $AppSettings.Solace.VPNName))</dd></div>
      <div><dt>Route</dt><dd>$(Encode $Topic) &rarr; $(Encode $Queue)</dd></div>
      <div><dt>Partitions</dt><dd>$PartitionCount</dd></div>
      <div><dt>Load / run</dt><dd>$Count msgs &middot; $KeyCount keys</dd></div>
      <div><dt>Simulated work</dt><dd>$($AppSettings.Subscriber.SimulatedHandlerWorkMinMs)&ndash;$($AppSettings.Subscriber.SimulatedHandlerWorkMaxMs) ms</dd></div>
      <div><dt>Run date</dt><dd>$RunDate</dd></div>
    </dl>
  </header>

  <section class="headline-stats">
    <div class="stat-tile">
      <p class="stat-value tabular">${fastestSpeedup}&times;</p>
      <p class="stat-label">throughput, fastest vs. slowest configuration<br>in this sweep</p>
    </div>
    <div class="stat-tile">
      <p class="stat-value tabular">$([math]::Round(($Results | Measure-Object -Property ElapsedS -Minimum).Minimum, 1))s</p>
      <p class="stat-label">fastest time to drain a $Count-message backlog<br>across all configurations tried</p>
    </div>
    <div class="stat-tile $(if ($totalViolations -eq 0) { 'status-good' } else { 'status-bad' })">
      <p class="stat-value tabular">$totalViolations</p>
      <p class="stat-label">ordering violation$(if ($totalViolations -ne 1) { "s" }),<br>across $totalMessages messages handled in total</p>
    </div>
  </section>

  <section class="chart-section">
    <h2>Throughput by instance count and per-instance concurrency</h2>
    <p class="chart-caption">Messages handled per second (aggregate across all instances in a configuration) while draining the backlog, grouped by subscriber process count.</p>
    <div class="legend">$($legendItems -join "`n")</div>
    <div class="chart-frame">
$throughputChart
    </div>
  </section>

  <section class="table-section">
    <h2>Full results</h2>
    <p class="section-intro">One trial per configuration. &ldquo;Busy&rdquo; instances are those that were actually assigned at least one partition; latency columns are the range across busy instances (min p50, max p99), not a merged percentile - see notes below.</p>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Instances</th><th>Concurrency</th><th>Total lanes</th><th>Busy</th><th>Messages</th>
            <th>Elapsed</th><th>Throughput</th><th>Violations</th><th>Min p50</th><th>Max p99</th>
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
      <li><strong>One trial per configuration.</strong> These are single runs, not averages across repeated trials; treat the curve as indicative, not a precise scaling law.</li>
      <li><strong>Elapsed is measured by wall clock, not by summing per-instance figures.</strong> With multiple processes running concurrently, one instance's own first-to-last-handled span doesn't necessarily cover the whole run, so <code>Elapsed</code> is the time from when the subscriber processes started consuming to when the broker (via SEMP) reported the queue fully drained.</li>
      <li><strong>Ordering.</strong> Every message's partition key is its <code>OrderId</code> (set on publish - see <code>messaging-lab.orders.OrderKeySelector</code> and <code>SolaceMessageSender&lt;T&gt;</code>'s optional partition-key selector), so the broker guarantees same-<code>OrderId</code> messages land in one partition and are therefore only ever seen by one subscriber process. $violationsLine</li>
      <li><strong>The idle-consumer ceiling.</strong> $saturationNote</li>
      <li><strong>Latency is a cross-instance range, not a merged percentile.</strong> Each instance computes its own p50/p99 independently over only the messages it personally handled; combining raw per-message latencies across processes to compute one true global percentile isn't done here, so <code>Min p50</code>/<code>Max p99</code> should be read as a range, not a single statistic.</li>
      <li><strong>The simulated handler cost is what makes this comparison meaningful.</strong> <code>SimulatedHandlerWorkMinMs</code>/<code>MaxMs</code> ($($AppSettings.Subscriber.SimulatedHandlerWorkMinMs)&ndash;$($AppSettings.Subscriber.SimulatedHandlerWorkMaxMs) ms, uniform random) stands in for real per-message work &mdash; an external call, a database write.</li>
    </ol>
  </section>

  <footer>
    <span>messaging-lab.solace.loadgen</span>
    <span>&rarr;</span>
    <span>$(Encode $Topic)</span>
    <span>&rarr;</span>
    <span>$(Encode $Queue) ($PartitionCount partitions)</span>
    <span>&rarr;</span>
    <span>N &times; messaging-lab.solace.subscriber</span>
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
