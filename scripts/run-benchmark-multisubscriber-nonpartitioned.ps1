<#
.SYNOPSIS
    Companion to run-benchmark-multisubscriber.ps1: runs the same instance-count x per-instance
    lane-count sweep, but against a plain non-exclusive, non-partitioned queue (CHANGED by
    default) instead of a partitioned one - specifically to measure whether, and how often,
    competing consumers with no partition-key routing actually produce cross-process ordering
    violations. Writes a self-contained HTML report to reports/.

.DESCRIPTION
    On a non-exclusive queue with no partitions, the broker round-robins deliveries across bound
    consumers with no idea what key is inside each message, so two messages sharing an OrderId can
    land on two different subscriber processes and be handled in either order. Each subscriber's
    own OrderingValidator only ever sees the subset of messages *it* received, so it can't detect
    this - two processes each seeing a locally-monotonic sequence can still have handled messages
    globally out of order. To actually measure this, OrderHandler now logs a per-message
    "HANDLED OrderId=... Sequence=..." line; this script merges every instance's log file for a
    configuration's time window by timestamp and re-checks per-key sequence order over that merged,
    true-arrival-order stream - the same check run-benchmark-multisubscriber.ps1 gets "for free"
    from partitioning, done by hand here since nothing at the broker level provides it.

.PARAMETER InstanceCounts
    Subscriber process counts to sweep. Default 1, 2, 4, 8, 16 (no partition count to derive this
    from, unlike the partitioned-queue script - there's no idle-consumer ceiling on a plain queue).

.PARAMETER Concurrencies
    Per-instance lane count to sweep (each instance binds SolaceConcurrentSubscriber<T> with this
    many worker lanes). Default 1,2,4,8. Every (InstanceCount, Concurrency) pair is run.

.PARAMETER Count
    Messages published per configuration.

.PARAMETER KeyCount
    Distinct OrderId keys per configuration, round-robin.

.PARAMETER Queue / Topic
    The plain (non-partitioned) queue and the topic it's subscribed to. Defaults to CHANGED /
    data-changed - the same pair the single-subscriber benchmark uses. CHANGED must be
    non-exclusive for this to mean anything (an exclusive queue only ever has one active consumer
    regardless of how many bind, which wouldn't exercise competing-consumer behavior at all).

.PARAMETER DrainTimeoutSeconds
    Max seconds to wait for a single configuration's backlog to fully drain before giving up on it.
    Defaults (0) to a value scaled from Count and the subscriber's SimulatedHandlerWorkMaxMs. A
    timeout aborts the whole sweep rather than continuing (see run-benchmark-multisubscriber.ps1's
    history for why: silently continuing on a timeout mixes one configuration's leftover backlog
    into the next).

.PARAMETER SempBaseUrl / SempUser / SempPassword
    Broker admin SEMP endpoint used to poll queue depth. Defaults match the local Docker broker
    documented in scripts/start-solace.ps1.

.EXAMPLE
    ./scripts/run-benchmark-multisubscriber-nonpartitioned.ps1

.EXAMPLE
    ./scripts/run-benchmark-multisubscriber-nonpartitioned.ps1 -InstanceCounts 2,4,8 -Concurrencies 1
#>

param(
    [int[]]$InstanceCounts = @(1, 2, 4, 8, 16),
    [int[]]$Concurrencies = @(1, 2, 4, 8),
    [int]$Count = 1000,
    [int]$KeyCount = 64,
    [string]$Queue = "CHANGED",
    [string]$Topic = "data-changed",
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

if ($DrainTimeoutSeconds -le 0) {
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
    $q = Invoke-RestMethod -Uri $QueueMonitorUrl -Headers $AuthHeader
}
catch {
    Write-Error "Could not reach queue '$Queue' via SEMP at $QueueMonitorUrl. Is the broker running (./scripts/start-solace.ps1), and does '$Queue' exist as a non-exclusive queue subscribed to topic '$Topic'? See README > Comparing multiple subscribers on a non-partitioned queue."
    exit 1
}
if ($q.data.accessType -ne "non-exclusive") {
    Write-Error "Queue '$Queue' has accessType='$($q.data.accessType)', not 'non-exclusive'. On an exclusive queue only one bound consumer is ever active regardless of how many processes bind, so this sweep wouldn't exercise competing-consumer behavior at all. Change its access type first."
    exit 1
}
if ($q.data.msgSpoolUsage -ne 0) {
    Write-Error "Queue '$Queue' is not empty (msgSpoolUsage=$($q.data.msgSpoolUsage)). Drain it before benchmarking so results aren't mixed with leftover messages."
    exit 1
}

$totalRuns = $InstanceCounts.Count * $Concurrencies.Count
Write-Host "Sweeping $($InstanceCounts.Count) instance count(s) x $($Concurrencies.Count) concurrency level(s) = $totalRuns configuration(s) against non-partitioned queue '$Queue'."
Write-Host "Instance counts: $($InstanceCounts -join ', ')  |  Concurrencies: $($Concurrencies -join ', ')"

# --- Build once, up front -----------------------------------------------------

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
$HandledPattern = '^(?<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [+\-]\d{2}:\d{2})\s+\[INF\]\s+\[[^\]]*\][^:]*:\s*HANDLED OrderId=(?<oid>\S+) Sequence=(?<seq>\d+)'

# Reads instance $instanceId's log file and returns every HANDLED event whose timestamp falls
# within [$Start, $End] (both local DateTime, with a small buffer either side). Each instance
# reuses the same daily log file across every configuration in the sweep, so this time-window
# filter - not a fresh file per run - is what isolates one configuration's events from the rest.
function Get-HandledEvents {
    param([int]$InstanceId, [datetime]$Start, [datetime]$End)

    $file = Get-ChildItem -Path $LogsDir -Filter "subscriber-$InstanceId-*.log" -ErrorAction SilentlyContinue | Select-Object -First 1
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

# Merges every busy instance's HANDLED events for this configuration into one true-arrival-order
# stream (by timestamp, across process boundaries) and re-runs the same per-key monotonic-sequence
# check SolaceConcurrentSubscriber<T>'s in-process OrderingValidator does - just globally instead
# of per-process, which is the only way to actually see a cross-process reordering happen.
function Measure-GlobalOrderingViolations {
    param([int]$InstanceCount, [datetime]$Start, [datetime]$End)

    $allEvents = 1..$InstanceCount | ForEach-Object { Get-HandledEvents -InstanceId $_ -Start $Start -End $End }
    $sorted = @($allEvents | Sort-Object Timestamp)

    $lastSeq = @{}
    $violations = 0
    foreach ($e in $sorted) {
        if ($lastSeq.ContainsKey($e.OrderId) -and $e.Sequence -le $lastSeq[$e.OrderId]) {
            $violations++
        }
        else {
            $lastSeq[$e.OrderId] = $e.Sequence
        }
    }

    [pscustomobject]@{ EventCount = $sorted.Count; Violations = $violations }
}

# Starts $InstanceCount subscriber processes bound to the queue, waits for the broker to report
# it fully drained, then stops them and parses each one's last metrics line, plus the global
# cross-process ordering check above over the same time window.
function Invoke-MultiSubscriberRun {
    param([int]$InstanceCount, [int]$Concurrency)

    $configStart = Get-Date

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
    $configEnd = Get-Date
    if ($waited -ge $DrainTimeoutSeconds) {
        $jobs | Stop-Job | Out-Null
        $jobs | Remove-Job -Force
        throw "Timed out after ${DrainTimeoutSeconds}s waiting for InstanceCount=$InstanceCount/Concurrency=$Concurrency to drain (queue usage=$(Get-QueueSpoolUsage)). Aborting the sweep rather than risk mixing this backlog into the next configuration - increase -DrainTimeoutSeconds and re-run."
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

    $global = Measure-GlobalOrderingViolations -InstanceCount $InstanceCount -Start $configStart -End $configEnd

    [pscustomobject]@{
        ElapsedS          = $drainStopwatch.Elapsed.TotalSeconds
        PerInstance       = @($perInstance)
        GlobalViolations  = $global.Violations
        GlobalEventCount  = $global.EventCount
    }
}

function Add-Result {
    param([int]$InstanceCount, [int]$Concurrency, $Run)

    $totalMessages = ($Run.PerInstance | Measure-Object -Property Messages -Sum).Sum
    $perProcessViolations = ($Run.PerInstance | Measure-Object -Property Violations -Sum).Sum
    $busy = @($Run.PerInstance | Where-Object { $_.Messages -gt 0 })
    $minP50 = if ($busy.Count -gt 0) { ($busy | Measure-Object -Property P50Ms -Minimum).Minimum } else { 0.0 }
    $maxP99 = if ($busy.Count -gt 0) { ($busy | Measure-Object -Property P99Ms -Maximum).Maximum } else { 0.0 }

    if ($totalMessages -ne $Count) {
        Write-Warning "Instances=$InstanceCount Concurrency=$Concurrency reported $totalMessages of $Count published messages handled across all instances - the snapshot may not reflect the full drain."
    }
    if ($Run.GlobalEventCount -ne $Count) {
        Write-Warning "Instances=$InstanceCount Concurrency=${Concurrency}: the merged HANDLED-log check found $($Run.GlobalEventCount) of $Count events - the global ordering check for this row may be incomplete (log rotation or a missed file is the likely cause)."
    }

    $script:Results += [pscustomobject]@{
        Instances             = $InstanceCount
        Concurrency           = $Concurrency
        BusyInstances         = $busy.Count
        Messages              = $totalMessages
        ElapsedS              = $Run.ElapsedS
        Rate                  = if ($Run.ElapsedS -gt 0) { $totalMessages / $Run.ElapsedS } else { 0.0 }
        PerProcessViolations  = $perProcessViolations
        GlobalViolations      = $Run.GlobalViolations
        MinP50Ms              = $minP50
        MaxP99Ms              = $maxP99
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

$ConcurrencyPalette = @{}
$Palette = @('#8b93a0', '#d68a3f', '#bf7228', '#a35a16', '#7a3f0a', '#c96f22', '#8a4a12', '#5c2b06')
$paletteIdx = 0
foreach ($c in $Concurrencies) {
    $ConcurrencyPalette[$c] = $Palette[$paletteIdx % $Palette.Count]
    $paletteIdx++
}

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
$totalPerProcessViolations = ($Results | Measure-Object -Property PerProcessViolations -Sum).Sum
$totalGlobalViolations = ($Results | Measure-Object -Property GlobalViolations -Sum).Sum

# --- Report: assemble HTML -----------------------------------------------------

$ReportsDir = Join-Path $RepoRoot "reports"
New-Item -ItemType Directory -Path $ReportsDir -Force | Out-Null
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$RunDate = Get-Date -Format "yyyy-MM-dd"
$ReportPath = Join-Path $ReportsDir "benchmark-multisubscriber-nonpartitioned-$Timestamp.html"

$rows = New-Object System.Collections.Generic.List[string]
foreach ($r in ($Results | Sort-Object Instances, Concurrency)) {
    $color = $ConcurrencyPalette[$r.Concurrency]
    $globalClass = if ($r.GlobalViolations -eq 0) { 'violations-ok' } else { 'violations-bad' }
    $mismatchFlag = if ($r.GlobalViolations -gt $r.PerProcessViolations) {
        " <span class=`"idle-flag`">(missed by per-process check)</span>"
    }
    else { "" }
    $rows.Add(@"
          <tr>
            <td><span class="swatch" style="background:$color"></span>$($r.Instances)</td>
            <td class="tabular">$($r.Concurrency)</td>
            <td class="tabular">$($r.Instances * $r.Concurrency)</td>
            <td class="tabular">$($r.Messages)</td>
            <td class="tabular">$([math]::Round($r.ElapsedS, 2))s</td>
            <td class="tabular">$([math]::Round($r.Rate, 1))/s</td>
            <td class="tabular">$($r.PerProcessViolations)</td>
            <td class="tabular $globalClass">$($r.GlobalViolations)$mismatchFlag</td>
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

$violationsLine = if ($totalGlobalViolations -eq 0) {
    "The merged, cross-process ordering check found zero violations across all $($Results.Count) configurations ($totalMessages messages) - on this run, the broker's round-robin delivery happened not to split any key's messages across two processes in a way that mattered. That's a property of this particular run's timing and message distribution, not a guarantee this queue provides - re-run with more keys, more instances, or more simulated handler-work variance to make a split more likely."
}
else {
    "The merged, cross-process ordering check found <strong>$totalGlobalViolations</strong> ordering violation(s) across $totalMessages messages handled - real reordering that the per-process check (which found $totalPerProcessViolations) mostly or entirely missed, because it can only ever compare a message against others handled by that same process."
}

$html = @"
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Multi-Subscriber Benchmark (Non-Partitioned) - $Timestamp</title>
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
  .idle-flag { color: var(--bad); font-size: 0.82em; }

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
    <h1>Multi-Subscriber Benchmark (Non-Partitioned)</h1>
    <p class="dek">
      $($Results.Count) configuration$(if ($Results.Count -ne 1) { "s" }) of a $Count-message backlog, competing consumers on a
      plain non-exclusive queue with <strong>no partition-key routing</strong> &mdash; the companion
      to the partitioned-queue benchmark, run to measure whether ordering actually breaks without it,
      using a cross-process log merge instead of each process's own (insufficient) ordering check.
    </p>
    <dl class="run-meta">
      <div><dt>Broker</dt><dd>$(Encode $AppSettings.Solace.Host) ($(Encode $AppSettings.Solace.VPNName))</dd></div>
      <div><dt>Route</dt><dd>$(Encode $Topic) &rarr; $(Encode $Queue)</dd></div>
      <div><dt>Access type</dt><dd>non-exclusive, 0 partitions</dd></div>
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
      <p class="stat-value tabular">$totalPerProcessViolations</p>
      <p class="stat-label">ordering violations by each process's own<br>(insufficient) per-process check</p>
    </div>
    <div class="stat-tile $(if ($totalGlobalViolations -eq 0) { 'status-good' } else { 'status-bad' })">
      <p class="stat-value tabular">$totalGlobalViolations</p>
      <p class="stat-label">ordering violations by the merged,<br>cross-process check - the real number</p>
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
    <p class="section-intro">One trial per configuration. "Per-process viol." sums what each instance's own OrderingValidator reported; "Global viol." is the merged cross-process check described above - the one that actually matters here. Latency columns are the range across busy instances (min p50, max p99), not a merged percentile.</p>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Instances</th><th>Concurrency</th><th>Total lanes</th><th>Messages</th>
            <th>Elapsed</th><th>Throughput</th><th>Per-process viol.</th><th>Global viol.</th><th>Min p50</th><th>Max p99</th>
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
      <li><strong>One trial per configuration.</strong> These are single runs, not averages across repeated trials; whether a given configuration produced violations can vary run to run, since it depends on the broker's real-time delivery timing and each message's randomized simulated work duration.</li>
      <li><strong>The point of this run.</strong> $violationsLine</li>
      <li><strong>How the global check works.</strong> <code>OrderHandler</code> logs a structured <code>HANDLED OrderId=... Sequence=...</code> line for every message. This script reads every instance's log file for a configuration's time window, merges all instances' events by timestamp into one true-arrival-order stream, and walks it checking that each key's <code>Sequence</code> strictly increases - exactly what a partitioned queue's per-process check already gets by construction, done here after the fact because nothing at the broker or client level provides it on a plain queue.</li>
      <li><strong>Elapsed is measured by wall clock</strong>, from when the subscriber processes start consuming to when the broker (via SEMP) reports the queue fully drained - not any single instance's own tracked elapsed figure.</li>
      <li><strong>Compare against the partitioned-queue report</strong> (see README > Comparing multiple subscribers) run at the same Count/KeyCount/instance-and-lane sweep: same throughput-scaling shape expected, but that report's ordering guarantee holds by construction, while this one's has to be checked for after the fact - and may or may not find anything, depending on timing.</li>
    </ol>
  </section>

  <footer>
    <span>messaging-lab.solace.loadgen</span>
    <span>&rarr;</span>
    <span>$(Encode $Topic)</span>
    <span>&rarr;</span>
    <span>$(Encode $Queue) (non-exclusive, non-partitioned)</span>
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
