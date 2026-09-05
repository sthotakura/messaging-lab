<#
.SYNOPSIS
    Runs an end-to-end SolaceConcurrentSubscriber<T> vs SolaceSequentialSubscriber<T> benchmark
    (publish a batch, drain it, repeat per configuration) and writes an HTML report under reports/.

.PARAMETER Count
    Messages published per configuration.

.PARAMETER KeyCount
    Distinct OrderId keys per configuration, round-robin.

.PARAMETER Concurrencies
    Lane counts to benchmark SolaceConcurrentSubscriber<T> at.

.PARAMETER SkipSequential
    Skip the SolaceSequentialSubscriber<T> baseline run.

.PARAMETER DrainTimeoutSeconds
    Max seconds to wait for a single configuration's backlog to fully drain before giving up on it.

.PARAMETER SempBaseUrl / SempUser / SempPassword
    Broker admin SEMP endpoint used to poll queue depth. Defaults match the local Docker broker
    documented in scripts/start-solace.ps1.

.EXAMPLE
    ./scripts/run-benchmark.ps1

.EXAMPLE
    ./scripts/run-benchmark.ps1 -Count 800 -Concurrencies 2,4,8 -SkipSequential
#>

param(
    [int]$Count = 200,
    [int]$KeyCount = 16,
    [int[]]$Concurrencies = @(1, 2, 4, 8),
    [switch]$SkipSequential,
    [int]$DrainTimeoutSeconds = 300,
    [string]$SempBaseUrl = "http://localhost:8080",
    [string]$SempUser = "admin",
    [string]$SempPassword = "admin"
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$SubscriberDir = Join-Path $RepoRoot "messaging-lab.solace.subscriber"
$LoadgenDir = Join-Path $RepoRoot "messaging-lab.solace.loadgen"

$AppSettings = Get-Content (Join-Path $SubscriberDir "appsettings.json") -Raw | ConvertFrom-Json
$LoadGenAppSettings = Get-Content (Join-Path $LoadgenDir "appsettings.json") -Raw | ConvertFrom-Json
$Queue = $AppSettings.Subscriber.Queue
$Topic = $LoadGenAppSettings.LoadGen.Topic

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
    Write-Error "Could not reach queue '$Queue' via SEMP at $QueueMonitorUrl. Is the broker running (./scripts/start-solace.ps1), and does the queue exist with a subscription to the load generator's topic? See README > Comparing the subscribers."
    exit 1
}
if ($usage -ne 0) {
    Write-Error "Queue '$Queue' is not empty (msgSpoolUsage=$usage). Drain it before benchmarking so results aren't mixed with leftover messages."
    exit 1
}

# --- Helpers -----------------------------------------------------------------

function Publish-Batch {
    Push-Location $LoadgenDir
    try {
        dotnet run -- --LoadGen:Count $Count --LoadGen:KeyCount $KeyCount | Out-Null
    }
    finally {
        Pop-Location
    }
}

# Runs the subscriber against whatever is currently on the queue, polling the broker for it to
# fully drain, then stops the subscriber and parses its last periodic metrics line.
function Invoke-SubscriberRun {
    param([bool]$UseConcurrent, [int]$Concurrency)

    $extraArgs = @("--Subscriber:UseConcurrentSubscriber", $(if ($UseConcurrent) { "true" } else { "false" }))
    if ($UseConcurrent) { $extraArgs += @("--Subscriber:Concurrency", "$Concurrency") }

    $job = Start-Job -ScriptBlock {
        param($subscriberDir, $extraArgs)
        Set-Location $subscriberDir
        dotnet run -- @extraArgs
    } -ArgumentList $SubscriberDir, $extraArgs

    $waited = 0
    while ($waited -lt $DrainTimeoutSeconds) {
        Start-Sleep -Seconds 1
        $waited++
        if ((Get-QueueSpoolUsage) -eq 0) { break }
    }
    if ($waited -ge $DrainTimeoutSeconds) {
        Write-Warning "Timed out after ${DrainTimeoutSeconds}s waiting for this configuration's backlog to drain; reporting whatever it reached."
    }

    # Give the periodic metrics reporter at least one full interval to log a snapshot reflecting the
    # drain above - Stop-Job kills the process without a graceful shutdown, so the final Report() in
    # MetricsReportingService's finally block never runs; without this wait we'd grab a stale mid-run
    # snapshot instead of one reflecting the fully-drained count.
    Start-Sleep -Seconds ([int]$AppSettings.Subscriber.MetricsReportIntervalSeconds + 1)
    Stop-Job $job | Out-Null
    $output = Receive-Job $job
    Remove-Job -Job $job -Force

    $pattern = 'Handled (\d+) messages in ([\d:.]+) \(([\d.]+) msgs/sec\) - ordering violations: (\d+), latency p50=([\d.]+)ms p99=([\d.]+)ms'
    $match = $output | Select-String -Pattern $pattern | Select-Object -Last 1
    if (-not $match) {
        Write-Warning "No metrics line found for this run; it may not have handled any messages."
        return $null
    }

    $g = $match.Matches[0].Groups
    [pscustomobject]@{
        Messages   = [int]$g[1].Value
        ElapsedS   = [TimeSpan]::Parse($g[2].Value).TotalSeconds
        Rate       = [double]$g[3].Value
        Violations = [int]$g[4].Value
        P50Ms      = [double]$g[5].Value
        P99Ms      = [double]$g[6].Value
    }
}

function Add-Result {
    param([string]$Label, [string]$Lanes, $Metrics)
    if (-not $Metrics) { return }
    if ($Metrics.Messages -ne $Count) {
        Write-Warning "$Label $Lanes reported $($Metrics.Messages) of $Count published messages handled - the snapshot may not reflect the full drain."
    }
    $script:Results += [pscustomobject]@{
        Label      = $Label
        Lanes      = $Lanes
        Messages   = $Metrics.Messages
        ElapsedS   = $Metrics.ElapsedS
        Rate       = $Metrics.Rate
        Violations = $Metrics.Violations
        P50Ms      = $Metrics.P50Ms
        P99Ms      = $Metrics.P99Ms
    }
}

# --- Run the matrix ----------------------------------------------------------

$Results = @()

if (-not $SkipSequential) {
    Write-Host "=== Sequential: publishing $Count messages across $KeyCount keys ==="
    Publish-Batch
    Write-Host "=== Sequential: draining ==="
    Add-Result -Label "Sequential" -Lanes "-" -Metrics (Invoke-SubscriberRun -UseConcurrent $false -Concurrency 0)
}

foreach ($n in $Concurrencies) {
    Write-Host "=== Concurrent (n=$n): publishing $Count messages across $KeyCount keys ==="
    Publish-Batch
    Write-Host "=== Concurrent (n=$n): draining ==="
    Add-Result -Label "Concurrent" -Lanes "$n" -Metrics (Invoke-SubscriberRun -UseConcurrent $true -Concurrency $n)
}

if ($Results.Count -eq 0) {
    Write-Error "No configuration produced a result; nothing to report."
    exit 1
}

# --- Report: shared helpers ---------------------------------------------------

function Encode([string]$s) { [System.Net.WebUtility]::HtmlEncode("$s") }

# Index 0 is reserved for the sequential baseline's neutral gray; the rest is an amber ramp
# (darker = more lanes), cycled if there are more concurrent configurations than palette slots.
# Shared by both charts and the results table so a configuration's color is consistent everywhere.
$Palette = @('#8b93a0', '#d68a3f', '#bf7228', '#a35a16', '#7a3f0a', '#c96f22', '#8a4a12', '#5c2b06')

function Get-PaletteColor {
    param([string]$Label, [int]$Index)
    if ($Label -eq "Sequential") { return $Palette[0] }
    return $Palette[1 + ($Index % ($Palette.Count - 1))]
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

function New-ThroughputChartSvg {
    param($Results)

    $plotLeft = 46; $plotRight = 700; $baselineY = 264; $plotTop = 26
    $maxRate = ($Results | Measure-Object -Property Rate -Maximum).Maximum
    $ticks = Get-NiceTicks -maxValue ($maxRate * 1.15)
    $yScale = ($baselineY - $plotTop) / $ticks.DomainMax

    $bandWidth = ($plotRight - $plotLeft) / $Results.Count
    $barWidth = [math]::Min(58, $bandWidth * 0.6)

    $svg = New-Object System.Collections.Generic.List[string]
    $svg.Add("<svg viewBox='0 0 720 344' xmlns='http://www.w3.org/2000/svg' role='img' aria-label='Bar chart of throughput in messages per second'>")
    $svg.Add("<text x='14' y='$($plotTop - 6)' class='axis-label'>msgs/sec</text>")

    foreach ($t in $ticks.Ticks) {
        $y = $baselineY - ($t * $yScale)
        $svg.Add("<line x1='$plotLeft' x2='$plotRight' y1='$y' y2='$y' class='grid-line' />")
        $svg.Add("<text x='$($plotLeft - 10)' y='$($y + 3)' text-anchor='end' class='axis-label'>$t</text>")
    }

    $firstConcurrentIdx = -1
    for ($idx = 0; $idx -lt $Results.Count; $idx++) {
        $r = $Results[$idx]
        $bandX = $plotLeft + $idx * $bandWidth
        $barX = $bandX + ($bandWidth - $barWidth) / 2
        $barH = [math]::Max($r.Rate * $yScale, 1)
        $barY = $baselineY - $barH
        $color = Get-PaletteColor -Label $r.Label -Index $idx

        $svg.Add("<rect x='$barX' y='$barY' width='$barWidth' height='$barH' rx='4' fill='$color' />")
        $svg.Add("<text x='$($barX + $barWidth / 2)' y='$($barY - 8)' text-anchor='middle' class='bar-value'>$([math]::Round($r.Rate, 1))</text>")

        $catLabel = if ($r.Label -eq "Sequential") { "Sequential" } else { "Concurrent" }
        $catSub = if ($r.Label -eq "Sequential") { "single-threaded" } else { "n = $($r.Lanes)" }
        $svg.Add("<text x='$($bandX + $bandWidth / 2)' y='$($baselineY + 20)' text-anchor='middle' class='cat-label'>$catLabel</text>")
        $svg.Add("<text x='$($bandX + $bandWidth / 2)' y='$($baselineY + 34)' text-anchor='middle' class='cat-sub'>$catSub</text>")

        if ($r.Label -ne "Sequential" -and $firstConcurrentIdx -eq -1) { $firstConcurrentIdx = $idx }
    }

    if ($firstConcurrentIdx -ge 0 -and $Results[0].Label -eq "Sequential") {
        $firstBand = $plotLeft + $firstConcurrentIdx * $bandWidth
        $lastBand = $plotLeft + $Results.Count * $bandWidth
        $by = $baselineY + 46
        $svg.Add("<path d='M $($firstBand + 6) $by L $($firstBand + 6) $($by + 6) L $($lastBand - 6) $($by + 6) L $($lastBand - 6) $by' class='group-bracket' />")
        $svg.Add("<text x='$(($firstBand + $lastBand) / 2)' y='$($by + 20)' text-anchor='middle' class='group-label'>SolaceConcurrentSubscriber&lt;T&gt; - worker lanes</text>")
    }

    $svg.Add("</svg>")
    return ($svg -join "`n")
}

function New-LatencyChartSvg {
    param($Results)

    $plotLeft = 168; $plotRight = 700; $plotTop = 24; $rowH = 52
    $maxP99S = (($Results | Measure-Object -Property P99Ms -Maximum).Maximum) / 1000
    $ticks = Get-NiceTicks -maxValue ($maxP99S * 1.1) -targetCount 6
    $xScale = ($plotRight - $plotLeft) / $ticks.DomainMax
    $baselineY = $plotTop + $Results.Count * $rowH + 4

    $svg = New-Object System.Collections.Generic.List[string]
    $svg.Add("<svg viewBox='0 0 720 $($baselineY + 40)' xmlns='http://www.w3.org/2000/svg' role='img' aria-label='Dumbbell chart of p50 to p99 latency in seconds'>")

    foreach ($t in $ticks.Ticks) {
        $x = $plotLeft + $t * $xScale
        $svg.Add("<line x1='$x' x2='$x' y1='$($plotTop - 6)' y2='$baselineY' class='grid-line' />")
        $svg.Add("<text x='$x' y='$($baselineY + 16)' text-anchor='middle' class='axis-label'>$t</text>")
    }
    $svg.Add("<text x='$plotRight' y='$($baselineY + 32)' text-anchor='end' class='axis-label'>seconds</text>")

    for ($idx = 0; $idx -lt $Results.Count; $idx++) {
        $r = $Results[$idx]
        $y = $plotTop + $idx * $rowH + $rowH / 2 - 6
        $x50 = $plotLeft + ($r.P50Ms / 1000) * $xScale
        $x99 = $plotLeft + ($r.P99Ms / 1000) * $xScale
        $color = Get-PaletteColor -Label $r.Label -Index $idx
        $rowLabel = if ($r.Label -eq "Sequential") { "Sequential" } else { "Concurrent &middot; n=$($r.Lanes)" }

        $svg.Add("<text x='$($plotLeft - 14)' y='$($y + 4)' text-anchor='end' class='row-label'>$rowLabel</text>")
        $svg.Add("<line x1='$x50' x2='$x99' y1='$y' y2='$y' stroke='$color' stroke-width='3' stroke-linecap='round' />")
        $svg.Add("<circle cx='$x50' cy='$y' r='6' fill='$color' />")
        $svg.Add("<circle cx='$x99' cy='$y' r='6' fill='var(--surface)' stroke='$color' stroke-width='2.5' />")
        $svg.Add("<text x='$x50' y='$($y - 12)' text-anchor='middle' class='lat-value'>$([math]::Round($r.P50Ms / 1000, 1))s</text>")
        $svg.Add("<text x='$x99' y='$($y - 12)' text-anchor='middle' class='lat-value'>$([math]::Round($r.P99Ms / 1000, 1))s</text>")
    }

    $svg.Add("</svg>")
    return ($svg -join "`n")
}

# --- Report: compute headline figures -----------------------------------------

$baseline = $Results | Where-Object { $_.Label -eq "Sequential" } | Select-Object -First 1
$baselineIsSequential = $null -ne $baseline
if (-not $baseline) { $baseline = $Results | Sort-Object Rate | Select-Object -First 1 }

$fastest = $Results | Sort-Object Rate -Descending | Select-Object -First 1
$fastestSpeedup = [math]::Round($fastest.Rate / $baseline.Rate, 2)
$totalMessages = ($Results | Measure-Object -Property Messages -Sum).Sum
$totalViolations = ($Results | Measure-Object -Property Violations -Sum).Sum
$baselineVsLabel = if ($baselineIsSequential) { "the sequential baseline" } else { "the lowest-concurrency run (n=$($baseline.Lanes))" }
$fastestLabel = if ($fastest.Label -eq "Sequential") { "sequential" } else { "$($fastest.Lanes) lanes" }

# --- Report: assemble HTML -----------------------------------------------------

$ReportsDir = Join-Path $RepoRoot "reports"
New-Item -ItemType Directory -Path $ReportsDir -Force | Out-Null
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$RunDate = Get-Date -Format "yyyy-MM-dd"
$ReportPath = Join-Path $ReportsDir "benchmark-$Timestamp.html"

$rows = New-Object System.Collections.Generic.List[string]
for ($idx = 0; $idx -lt $Results.Count; $idx++) {
    $r = $Results[$idx]
    $speedup = [math]::Round($r.Rate / $baseline.Rate, 2)
    $color = Get-PaletteColor -Label $r.Label -Index $idx
    $rows.Add(@"
          <tr>
            <td><span class="swatch" style="background:$color"></span>$(Encode $r.Label)</td>
            <td class="tabular">$(Encode $r.Lanes)</td>
            <td class="tabular">$($r.Messages)</td>
            <td class="tabular">$([math]::Round($r.ElapsedS, 2))s</td>
            <td class="tabular">$([math]::Round($r.Rate, 1))/s</td>
            <td class="tabular">${speedup}&times;</td>
            <td class="tabular">$($r.P50Ms) ms</td>
            <td class="tabular">$($r.P99Ms) ms</td>
            <td class="tabular $(if ($r.Violations -eq 0) { 'violations-ok' } else { 'violations-bad' })">$($r.Violations)</td>
          </tr>
"@)
}

$legendItems = New-Object System.Collections.Generic.List[string]
$legendItems.Add('<span class="item"><span class="swatch" style="background:var(--baseline)"></span>Sequential (baseline)</span>')
for ($idx = 0; $idx -lt $Results.Count; $idx++) {
    $r = $Results[$idx]
    if ($r.Label -eq "Sequential") { continue }
    $legendItems.Add("<span class=`"item`"><span class=`"swatch`" style=`"background:$(Get-PaletteColor -Label $r.Label -Index $idx)`"></span>Concurrent, n=$($r.Lanes)</span>")
}

$throughputChart = New-ThroughputChartSvg -Results $Results
$latencyChart = New-LatencyChartSvg -Results $Results

$violationsLine = if ($totalViolations -eq 0) {
    "Zero ordering violations across all $($Results.Count) runs ($totalMessages messages) confirms that guarantee held."
}
else {
    "$totalViolations ordering violation(s) were observed across $totalMessages messages handled - investigate before trusting the concurrent subscriber's key-selector routing on this run."
}

$html = @"
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Subscriber Benchmark - $Timestamp</title>
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
  .page { max-width: 860px; margin: 0 auto; padding: 48px 24px 72px; display: flex; flex-direction: column; gap: 40px; }
  h1, h2 { font-family: var(--font-display); text-wrap: balance; margin: 0; }
  code { font-family: var(--font-mono); font-size: 0.92em; background: var(--surface-2); border: 1px solid var(--line); border-radius: 4px; padding: 0.05em 0.35em; }
  .tabular { font-variant-numeric: tabular-nums; }

  .masthead { display: flex; flex-direction: column; gap: 14px; padding-bottom: 28px; border-bottom: 1px solid var(--line); }
  .eyebrow { margin: 0; font-family: var(--font-mono); font-size: 0.78rem; letter-spacing: 0.09em; text-transform: uppercase; color: var(--accent); }
  .masthead h1 { font-size: clamp(1.9rem, 4vw, 2.5rem); font-weight: 700; letter-spacing: -0.01em; }
  .dek { max-width: 62ch; margin: 0; color: var(--ink-soft); font-size: 1.02rem; }
  .run-meta { margin: 6px 0 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; background: var(--surface); }
  .run-meta > div { padding: 10px 14px; border-left: 1px solid var(--line); }
  .run-meta > div:first-child { border-left: none; }
  .run-meta dt { margin: 0; font-family: var(--font-mono); font-size: 0.68rem; letter-spacing: 0.06em; text-transform: uppercase; color: var(--ink-faint); }
  .run-meta dd { margin: 3px 0 0; font-family: var(--font-mono); font-size: 0.86rem; color: var(--ink); }

  .headline-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 14px; }
  .stat-tile { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 18px 20px; }
  .stat-tile.status-good { background: var(--good-bg); border-color: var(--good); }
  .stat-tile.status-bad { background: var(--bad-bg); border-color: var(--bad); }
  .stat-value { margin: 0; font-family: var(--font-display); font-weight: 700; font-size: 2.05rem; letter-spacing: -0.01em; }
  .status-good .stat-value { color: var(--good); }
  .status-bad .stat-value { color: var(--bad); }
  .stat-label { margin: 6px 0 0; font-size: 0.84rem; color: var(--ink-soft); line-height: 1.4; }

  section h2 { font-size: 1.25rem; font-weight: 600; margin-bottom: 6px; }
  .chart-caption, .section-intro { max-width: 68ch; color: var(--ink-soft); font-size: 0.92rem; margin: 0 0 16px; }
  .legend { display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 12px; font-family: var(--font-mono); font-size: 0.76rem; color: var(--ink-soft); }
  .legend .item { display: inline-flex; align-items: center; gap: 6px; }
  .swatch { width: 10px; height: 10px; border-radius: 3px; display: inline-block; flex: none; margin-right: 6px; vertical-align: middle; }
  .chart-frame { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 18px 18px 6px; }
  .chart-frame svg { display: block; width: 100%; height: auto; }

  .axis-label { fill: var(--ink-faint); font-family: var(--font-mono); font-size: 10.5px; }
  .grid-line { stroke: var(--line); stroke-width: 1; }
  .bar-value { fill: var(--ink); font-family: var(--font-mono); font-weight: 600; font-size: 13px; }
  .cat-label { fill: var(--ink); font-family: var(--font-display); font-weight: 600; font-size: 12.5px; }
  .cat-sub { fill: var(--ink-faint); font-family: var(--font-mono); font-size: 10px; }
  .group-label { fill: var(--ink-faint); font-family: var(--font-mono); font-size: 10px; letter-spacing: 0.04em; }
  .group-bracket { stroke: var(--line-strong); stroke-width: 1; fill: none; }
  .row-label { fill: var(--ink); font-family: var(--font-display); font-weight: 600; font-size: 12.5px; }
  .lat-value { fill: var(--ink-soft); font-family: var(--font-mono); font-size: 10.5px; }

  .table-scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 10px; }
  table { width: 100%; border-collapse: collapse; background: var(--surface); font-size: 0.86rem; min-width: 640px; }
  thead th { text-align: right; font-family: var(--font-mono); font-weight: 500; font-size: 0.7rem; letter-spacing: 0.05em; text-transform: uppercase; color: var(--ink-faint); padding: 10px 14px; border-bottom: 1px solid var(--line-strong); white-space: nowrap; }
  thead th:first-child, thead th:nth-child(2) { text-align: left; }
  tbody td { text-align: right; font-family: var(--font-mono); padding: 10px 14px; border-bottom: 1px solid var(--line); white-space: nowrap; }
  tbody tr:last-child td { border-bottom: none; }
  tbody td:first-child, tbody td:nth-child(2) { text-align: left; font-family: var(--font-display); }
  .violations-ok { color: var(--good); font-weight: 600; }
  .violations-bad { color: var(--bad); font-weight: 600; }

  .notes-section ol { padding-left: 1.3em; margin: 0; display: flex; flex-direction: column; gap: 10px; max-width: 70ch; }
  .notes-section li { padding-left: 4px; }
  .notes-section li::marker { font-family: var(--font-mono); color: var(--ink-faint); }

  footer { padding-top: 20px; border-top: 1px solid var(--line); font-family: var(--font-mono); font-size: 0.78rem; color: var(--ink-faint); display: flex; flex-wrap: wrap; gap: 6px 10px; align-items: center; }
</style>
</head>
<body>
<div class="page">

  <header class="masthead">
    <p class="eyebrow">messaging-lab &middot; benchmark report</p>
    <h1>Subscriber Benchmark</h1>
    <p class="dek">
      $($Results.Count) run$(if ($Results.Count -ne 1) { "s" }) of a $Count-message backlog through
      <code>SolaceSequentialSubscriber&lt;T&gt;</code> and <code>SolaceConcurrentSubscriber&lt;T&gt;</code>
      at increasing lane counts &mdash; same broker, same load, same simulated per-message handler cost.
    </p>
    <dl class="run-meta">
      <div><dt>Broker</dt><dd>$(Encode $AppSettings.Solace.Host) ($(Encode $AppSettings.Solace.VPNName))</dd></div>
      <div><dt>Route</dt><dd>$(Encode $Topic) &rarr; $(Encode $Queue)</dd></div>
      <div><dt>Load / run</dt><dd>$Count msgs &middot; $KeyCount keys</dd></div>
      <div><dt>Delivery mode</dt><dd>$(Encode $LoadGenAppSettings.LoadGen.DeliveryMode)</dd></div>
      <div><dt>Simulated work</dt><dd>$($AppSettings.Subscriber.SimulatedHandlerWorkMinMs)&ndash;$($AppSettings.Subscriber.SimulatedHandlerWorkMaxMs) ms</dd></div>
      <div><dt>Run date</dt><dd>$RunDate</dd></div>
    </dl>
  </header>

  <section class="headline-stats">
    <div class="stat-tile">
      <p class="stat-value tabular">${fastestSpeedup}&times;</p>
      <p class="stat-label">throughput at $fastestLabel,<br>vs. $baselineVsLabel</p>
    </div>
    <div class="stat-tile">
      <p class="stat-value tabular">$([math]::Round($baseline.ElapsedS, 1))s &rarr; $([math]::Round(($Results | Measure-Object -Property ElapsedS -Minimum).Minimum, 1))s</p>
      <p class="stat-label">time to drain the same $Count-message backlog,<br>$(if ($baselineIsSequential) { "sequential" } else { "n=$($baseline.Lanes)" }) vs. fastest run</p>
    </div>
    <div class="stat-tile $(if ($totalViolations -eq 0) { 'status-good' } else { 'status-bad' })">
      <p class="stat-value tabular">$totalViolations</p>
      <p class="stat-label">ordering violation$(if ($totalViolations -ne 1) { "s" }),<br>across $totalMessages messages handled in total</p>
    </div>
  </section>

  <section class="chart-section">
    <h2>Throughput by configuration</h2>
    <p class="chart-caption">Messages handled per second while draining the backlog. The concurrent subscriber's bars are shaded by lane count &mdash; darker means more lanes doing the work.</p>
    <div class="legend">$($legendItems -join "`n")</div>
    <div class="chart-frame">
$throughputChart
    </div>
  </section>

  <section class="chart-section">
    <h2>End-to-end latency: p50 &rarr; p99</h2>
    <p class="chart-caption">Time from publish to handled, for the median and 99th-percentile message, in seconds. Every message was already sitting in the queue before the subscriber connected, so this is backlog-drain latency, not steady-state request latency &mdash; see the notes below.</p>
    <div class="legend">
      <span class="item"><span class="dot" style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--ink-soft);margin-right:6px;vertical-align:middle;"></span>p50 (median)</span>
      <span class="item"><span class="dot" style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--surface);border:2px solid var(--ink-soft);margin-right:6px;vertical-align:middle;"></span>p99 (tail)</span>
      <span class="item">color per row matches the throughput chart above</span>
    </div>
    <div class="chart-frame">
$latencyChart
    </div>
  </section>

  <section class="table-section">
    <h2>Full results</h2>
    <p class="section-intro">One trial per configuration; see method &amp; caveats below.</p>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Configuration</th><th>Lanes</th><th>Messages</th><th>Elapsed</th>
            <th>Throughput</th><th>Speedup</th><th>p50</th><th>p99</th><th>Violations</th>
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
      <li><strong>Latency here means backlog-drain time, not live request latency.</strong> All $Count messages for a run were published to <code>$(Encode $Topic)</code> before the subscriber connected, so p50/p99 mostly measure how long a message waited its turn in a full queue, not publish-to-handle time under steady, trickling traffic.</li>
      <li><strong>Ordering.</strong> <code>SolaceConcurrentSubscriber&lt;T&gt;</code> was run with a key selector that routes same-<code>OrderId</code> messages to one lane, in delivery order. $violationsLine</li>
      <li><strong>The simulated handler cost is what makes this comparison meaningful.</strong> <code>SimulatedHandlerWorkMinMs</code>/<code>MaxMs</code> ($($AppSettings.Subscriber.SimulatedHandlerWorkMinMs)&ndash;$($AppSettings.Subscriber.SimulatedHandlerWorkMaxMs) ms, uniform random) stands in for real per-message work &mdash; an external call, a database write. Without it, both subscriber types process trivial work at roughly the same rate, since there's nothing for concurrency to overlap.</li>
    </ol>
  </section>

  <footer>
    <span>messaging-lab.solace.loadgen</span>
    <span>&rarr;</span>
    <span>$(Encode $Topic)</span>
    <span>&rarr;</span>
    <span>messaging-lab.solace.subscriber</span>
    <span>&middot;</span>
    <span>$Count msgs &times; $($Results.Count) runs &middot; $KeyCount keys/run</span>
  </footer>

</div>
</body>
</html>
"@

$html | Set-Content -Path $ReportPath -Encoding utf8

Write-Host ""
Write-Host "Report written to $ReportPath"
