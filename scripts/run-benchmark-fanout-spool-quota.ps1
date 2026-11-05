<#
.SYNOPSIS
    Fan-out lab: shows how one queue's spool quota, plus one absent subscriber, affects the publisher
    and every healthy subscriber sharing the same topic. Writes a self-contained HTML report to reports/.

.DESCRIPTION
    One topic fans out to three queues, each with its own single-lane subscriber. The subscribers on
    FANOUT-A and FANOUT-B run from the start. The subscriber on FANOUT-C is not started until partway
    through, so its queue builds a backlog and, with a small quota, fills. Starting it late is the
    recovery phase.

    Each scenario recreates the queues and changes only FANOUT-C's configuration:

      control  Large quota, default discard behaviour. The same absent subscriber, but the quota is
               not reached during the run.
      reject   Small quota, default discard behaviour (when-queue-enabled).
      never    Small quota, reject-msg-to-sender-on-discard = never.

    A small quota and a modest rate reach the limit in seconds. The broker applies the same limit
    however large it is; a production-sized quota simply takes longer to reach.

    Only the FANOUT-A/B/C queues are touched. They're deleted and recreated per scenario and deleted
    at the end.

.PARAMETER Scenarios
    Which scenarios to run, in order. Default: control, reject, never.

.PARAMETER RatePerSecond / DurationSeconds / PayloadBytes
    Publisher settings. The rate must stay below what a single-lane subscriber can sustain, otherwise
    the healthy queues build a backlog of their own and confuse the result.

.PARAMETER SubscriberCStartSeconds
    Seconds after the publisher starts before FANOUT-C's subscriber is started.

.PARAMETER SmallQuotaMb / LargeQuotaMb
    FANOUT-C's quota in the small-quota scenarios, and the quota of every other queue.

.PARAMETER DrainTimeoutSeconds
    Max seconds to wait after the publisher finishes for all three queues to empty.

.PARAMETER SempBaseUrl / SempUser / SempPassword
    Broker admin SEMP endpoint. Defaults match the local Docker broker in scripts/start-solace.ps1.

.PARAMETER RenderFromResults
    Path to a results.json saved by an earlier run. Rebuilds the HTML report from it without
    touching the broker.

.EXAMPLE
    ./scripts/run-benchmark-fanout-spool-quota.ps1

.EXAMPLE
    ./scripts/run-benchmark-fanout-spool-quota.ps1 -Scenarios reject -DurationSeconds 45 -SubscriberCStartSeconds 30
#>

param(
    [ValidateSet("control", "reject", "never")] [string[]]$Scenarios = @("control", "reject", "never"),
    [int]$RatePerSecond = 500,
    [int]$DurationSeconds = 90,
    [int]$PayloadBytes = 1024,
    [int]$SubscriberCStartSeconds = 60,
    [int]$SmallQuotaMb = 5,
    [int]$LargeQuotaMb = 1000,
    [int]$DrainTimeoutSeconds = 120,
    [string]$SempBaseUrl = "http://localhost:8080",
    [string]$SempUser = "admin",
    [string]$SempPassword = "admin",
    [string]$RenderFromResults
)

$ErrorActionPreference = "Stop"

$Topic = "fanout-orders"
$QueueNames = @("FANOUT-A", "FANOUT-B", "FANOUT-C")

$ScenarioDefinitions = @{
    control = @{ Title = "Control: same dead subscriber, quota not reached"; QuotaMb = $LargeQuotaMb; Discard = "when-queue-enabled" }
    reject  = @{ Title = "Small quota, default discard behaviour"; QuotaMb = $SmallQuotaMb; Discard = "when-queue-enabled" }
    never   = @{ Title = "Small quota, discard behaviour set to never"; QuotaMb = $SmallQuotaMb; Discard = "never" }
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$SubscriberDir = Join-Path $RepoRoot "messaging-lab.solace.subscriber"
$FanoutDir = Join-Path $RepoRoot "messaging-lab.solace.fanout"
$ReportsDir = Join-Path $RepoRoot "reports"
$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$WorkDir = Join-Path ([System.IO.Path]::GetTempPath()) "messaging-lab-fanout-$Stamp"
New-Item -ItemType Directory -Force $WorkDir | Out-Null

$AuthHeader = @{
    Authorization = "Basic " + [Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("${SempUser}:${SempPassword}"))
}
$ConfigUrl = "$SempBaseUrl/SEMP/v2/config/msgVpns/default/queues"
$MonitorUrl = "$SempBaseUrl/SEMP/v2/monitor/msgVpns/default/queues"

# --- Broker helpers -----------------------------------------------------------

function Remove-LabQueues {
    foreach ($queue in $QueueNames) {
        try { Invoke-RestMethod -Method Delete -Uri "$ConfigUrl/$queue" -Headers $AuthHeader | Out-Null }
        catch { if ($_.Exception.Response.StatusCode.value__ -ne 400) { throw } }
    }
}

function New-LabQueue {
    param([string]$Name, [int]$QuotaMb, [string]$Discard)

    $body = @{
        queueName                          = $Name
        accessType                         = "exclusive"
        permission                         = "consume"
        ingressEnabled                     = $true
        egressEnabled                      = $true
        maxMsgSpoolUsage                   = $QuotaMb
        rejectMsgToSenderOnDiscardBehavior = $Discard
    } | ConvertTo-Json
    Invoke-RestMethod -Method Post -Uri $ConfigUrl -Headers $AuthHeader -ContentType "application/json" -Body $body | Out-Null

    $subscription = @{ subscriptionTopic = $Topic } | ConvertTo-Json
    Invoke-RestMethod -Method Post -Uri "$ConfigUrl/$Name/subscriptions" -Headers $AuthHeader -ContentType "application/json" -Body $subscription | Out-Null
}

function Get-QueueStats {
    param([string]$Name)
    $select = "msgSpoolUsage,spooledMsgCount,maxMsgSpoolUsageExceededDiscardedMsgCount,bindSuccessCount,txUnackedMsgCount"
    (Invoke-RestMethod -Uri "$MonitorUrl/$Name`?select=$select" -Headers $AuthHeader).data
}

# --- Process helpers ----------------------------------------------------------

function Start-Subscriber {
    param([string]$Queue, [string]$ScenarioDir)

    $arguments = @(
        "`"$SubscriberDll`""
        "--Subscriber:Queue", $Queue
        "--Subscriber:InstanceId", $Queue.ToLowerInvariant()
        "--Subscriber:UseConcurrentSubscriber", "true"
        "--Subscriber:Concurrency", "1"
        "--Subscriber:SimulatedHandlerWorkMinMs", "0"
        "--Subscriber:SimulatedHandlerWorkMaxMs", "0"
    )
    Start-Process dotnet -ArgumentList $arguments -WorkingDirectory $SubscriberDir -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $ScenarioDir "$Queue.stdout.log") `
        -RedirectStandardError (Join-Path $ScenarioDir "$Queue.stderr.log")
}

function Wait-ForBind {
    param([string]$Queue)
    for ($i = 0; $i -lt 60; $i++) {
        if ((Get-QueueStats $Queue).bindSuccessCount -gt 0) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "Subscriber on '$Queue' did not bind within 30s."
}

function Get-UnixMs { [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() }

function Get-HandledSequences {
    param([string]$Path)
    $sequences = [System.Collections.Generic.List[long]]::new()
    $pattern = [regex]'HANDLED OrderId=\S+ Sequence=(\d+)'
    foreach ($line in [System.IO.File]::ReadLines($Path)) {
        $match = $pattern.Match($line)
        if ($match.Success) { $sequences.Add([long]$match.Groups[1].Value) }
    }
    , $sequences
}

# --- One scenario -------------------------------------------------------------

function Invoke-Scenario {
    param([string]$Key)

    $definition = $ScenarioDefinitions[$Key]
    $scenarioDir = Join-Path $WorkDir $Key
    New-Item -ItemType Directory -Force $scenarioDir | Out-Null
    Write-Host "`n=== $Key : $($definition.Title) (FANOUT-C quota $($definition.QuotaMb) MB, discard '$($definition.Discard)') ==="

    Remove-LabQueues
    New-LabQueue "FANOUT-A" $LargeQuotaMb "when-queue-enabled"
    New-LabQueue "FANOUT-B" $LargeQuotaMb "when-queue-enabled"
    New-LabQueue "FANOUT-C" $definition.QuotaMb $definition.Discard

    $processes = [System.Collections.Generic.List[object]]::new()
    try {
        $processes.Add((Start-Subscriber "FANOUT-A" $scenarioDir))
        $processes.Add((Start-Subscriber "FANOUT-B" $scenarioDir))
        Wait-ForBind "FANOUT-A"
        Wait-ForBind "FANOUT-B"

        $publisherArguments = @(
            "`"$FanoutDll`""
            "--Fanout:Topic", $Topic
            "--Fanout:RatePerSecond", $RatePerSecond
            "--Fanout:DurationSeconds", $DurationSeconds
            "--Fanout:PayloadBytes", $PayloadBytes
            "--Fanout:OutputDirectory", "`"$scenarioDir`""
        )
        $startMs = Get-UnixMs
        $publisher = Start-Process dotnet -ArgumentList $publisherArguments -WorkingDirectory $FanoutDir -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $scenarioDir "publisher.stdout.log") `
            -RedirectStandardError (Join-Path $scenarioDir "publisher.stderr.log")
        $processes.Add($publisher)

        $samples = [System.Collections.Generic.List[object]]::new()
        $subscriberCStartMs = $null
        $publisherEndMs = $null
        $drainDeadlineMs = $null
        $drainedAtMs = $null

        while ($true) {
            $nowMs = Get-UnixMs
            $elapsedSeconds = ($nowMs - $startMs) / 1000

            if (-not $subscriberCStartMs -and $elapsedSeconds -ge $SubscriberCStartSeconds) {
                Write-Host "  t=$([int]$elapsedSeconds)s: starting FANOUT-C's subscriber"
                $processes.Add((Start-Subscriber "FANOUT-C" $scenarioDir))
                $subscriberCStartMs = Get-UnixMs
            }

            $stats = @{}
            foreach ($queue in $QueueNames) { $stats[$queue] = Get-QueueStats $queue }
            $samples.Add([pscustomobject]@{
                    utcMs = $nowMs
                    a     = $stats["FANOUT-A"]
                    b     = $stats["FANOUT-B"]
                    c     = $stats["FANOUT-C"]
                })

            if (-not $publisherEndMs -and $publisher.HasExited) {
                $publisherEndMs = Get-UnixMs
                $drainDeadlineMs = $publisherEndMs + ($DrainTimeoutSeconds * 1000)
                Write-Host "  t=$([int]$elapsedSeconds)s: publisher finished, waiting for queues to drain"
            }

            if ($publisherEndMs -and $subscriberCStartMs) {
                $usage = ($QueueNames | ForEach-Object { $stats[$_].msgSpoolUsage } | Measure-Object -Sum).Sum
                if ($usage -eq 0) { $drainedAtMs = $nowMs; break }
                if ($nowMs -ge $drainDeadlineMs) {
                    Write-Warning "$Key : queues still held $usage bytes after ${DrainTimeoutSeconds}s; handled counts below will be incomplete."
                    break
                }
            }

            $sleepMs = 1000 - ((Get-UnixMs) - $nowMs)
            if ($sleepMs -gt 0) { Start-Sleep -Milliseconds $sleepMs }
        }

        # Let the subscribers' console loggers finish before they're killed: their HANDLED lines are
        # the per-message evidence analysed below.
        Start-Sleep -Seconds 3
    }
    finally {
        foreach ($process in $processes) {
            if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
        }
        foreach ($process in $processes) { [void]$process.WaitForExit(5000) }
    }

    $finalStats = @{}
    foreach ($queue in $QueueNames) { $finalStats[$queue] = Get-QueueStats $queue }

    # --- Analysis ---
    $summary = Get-Content (Join-Path $scenarioDir "publisher-summary.json") -Raw | ConvertFrom-Json
    $rejected = [System.Collections.Generic.HashSet[long]]::new()
    foreach ($line in [System.IO.File]::ReadLines((Join-Path $scenarioDir "rejected-sequences.txt"))) {
        if ($line) { [void]$rejected.Add([long]$line) }
    }

    $queueResults = foreach ($queue in $QueueNames) {
        $handled = Get-HandledSequences (Join-Path $scenarioDir "$queue.stdout.log")
        $distinct = [System.Collections.Generic.HashSet[long]]::new($handled)
        $rejectedAndHandled = 0
        foreach ($sequence in $distinct) { if ($rejected.Contains($sequence)) { $rejectedAndHandled++ } }
        $errorText = Get-Content (Join-Path $scenarioDir "$queue.stderr.log") -Raw -ErrorAction SilentlyContinue
        if ($errorText) { Write-Warning "$queue stderr: $errorText" }
        [pscustomobject]@{
            queue              = $queue
            handled            = $handled.Count
            distinct           = $distinct.Count
            rejectedAndHandled = $rejectedAndHandled
            missed             = [long]$summary.Acked - $distinct.Count + $rejectedAndHandled
            spooled            = [long]$finalStats[$queue].spooledMsgCount
            discarded          = [long]$finalStats[$queue].maxMsgSpoolUsageExceededDiscardedMsgCount
            peakUsageBytes     = ($samples | ForEach-Object { $_.($queue.Substring(7).ToLowerInvariant()).msgSpoolUsage } | Measure-Object -Maximum).Maximum
        }
    }

    function ConvertTo-RelativeSeconds($value) {
        if ($null -eq $value) { return $null }
        $ms = ([DateTimeOffset]$value.ToUniversalTime()).ToUnixTimeMilliseconds()
        [math]::Round(($ms - $startMs) / 1000, 2)
    }

    $publisherRows = Import-Csv (Join-Path $scenarioDir "publisher.csv")

    [pscustomobject]@{
        key               = $Key
        title             = $definition.Title
        quotaMb           = $definition.QuotaMb
        discard           = $definition.Discard
        ratePerSecond     = $RatePerSecond
        payloadBytes      = $PayloadBytes
        durationSeconds   = $DurationSeconds
        subscriberCStartS = [math]::Round(($subscriberCStartMs - $startMs) / 1000, 2)
        drainedAtS        = if ($drainedAtMs) { [math]::Round(($drainedAtMs - $startMs) / 1000, 2) } else { $null }
        sent              = [long]$summary.Sent
        acked             = [long]$summary.Acked
        rejected          = [long]$summary.Rejected
        sendFailures      = [long]$summary.SendFailures
        noVerdict         = [long]$summary.NoVerdict
        firstRejectedS    = ConvertTo-RelativeSeconds $summary.FirstRejectedUtc
        lastRejectedS     = ConvertTo-RelativeSeconds $summary.LastRejectedUtc
        reasons           = $summary.Reasons
        queues            = @($queueResults)
        publisher         = @($publisherRows | ForEach-Object {
                [pscustomobject]@{
                    t        = [math]::Round(([long]$_.utcMs - $startMs) / 1000, 2)
                    sent     = [long]$_.sent
                    acked    = [long]$_.acked
                    rejected = [long]$_.rejected
                }
            })
        samples           = @($samples | ForEach-Object {
                [pscustomobject]@{
                    t      = [math]::Round(($_.utcMs - $startMs) / 1000, 2)
                    aUsage = $_.a.msgSpoolUsage; bUsage = $_.b.msgSpoolUsage; cUsage = $_.c.msgSpoolUsage
                    aSpool = $_.a.spooledMsgCount; bSpool = $_.b.spooledMsgCount; cSpool = $_.c.spooledMsgCount
                    cDiscard = $_.c.maxMsgSpoolUsageExceededDiscardedMsgCount
                }
            })
    }
}

# --- Report -------------------------------------------------------------------

function Encode([string]$s) { [System.Net.WebUtility]::HtmlEncode("$s") }
function Format-Count($value) { "{0:N0}" -f [long]$value }

function Get-ScenarioFinding {
    param($r)

    $a = $r.queues | Where-Object queue -eq "FANOUT-A"
    $b = $r.queues | Where-Object queue -eq "FANOUT-B"
    $c = $r.queues | Where-Object queue -eq "FANOUT-C"

    if ($r.rejected -gt 0) {
        $overlap = $a.rejectedAndHandled + $b.rejectedAndHandled
        return "The broker rejected $(Format-Count $r.rejected) of $(Format-Count $r.sent) messages between t=$($r.firstRejectedS)s and t=$($r.lastRejectedS)s. FANOUT-A and FANOUT-B missed $(Format-Count ($r.sent - $a.distinct)) and $(Format-Count ($r.sent - $b.distinct)) messages, though their subscribers were healthy throughout. Matching sequence numbers, $overlap of the rejected messages reached either of them. Rejections stopped $([math]::Round($r.lastRejectedS - $r.subscriberCStartS, 2))s after FANOUT-C's subscriber was launched, but this publisher doesn't resend, so the rejected messages stayed lost for all three queues."
    }
    if ($c.discarded -gt 0) {
        return "The publisher saw $(Format-Count $r.acked) ACKs and no rejections, and FANOUT-A and FANOUT-B handled $(Format-Count $a.distinct) and $(Format-Count $b.distinct). FANOUT-C lost $(Format-Count $c.discarded) messages. Nothing told the publisher; the only trace is the queue's discard counter on the broker."
    }
    $peakMb = [math]::Round($c.peakUsageBytes / 1MB, 1)
    return "No rejections and no discards. FANOUT-C's backlog peaked at $peakMb MB of its $($r.quotaMb) MB quota, and FANOUT-A, B and C handled $(Format-Count $a.distinct), $(Format-Count $b.distinct) and $(Format-Count $c.distinct) of the $(Format-Count $r.sent) messages sent."
}

function New-FanoutReport {
    param($Results, [string]$OutputPath)

    $first = $Results | Select-Object -First 1
    $byKey = @{}
    foreach ($r in $Results) { $byKey[$r.key] = $r }

    $tiles = ""
    if ($byKey.reject) {
        $r = $byKey.reject
        $a = $r.queues | Where-Object queue -eq "FANOUT-A"
        $tiles += "<div class='stat-tile status-bad'><p class='stat-value tabular'>$(Format-Count ($r.sent - $a.distinct))</p><p class='stat-label'>messages a <strong>healthy</strong> subscriber never received under the default setting</p></div>"
        $tiles += "<div class='stat-tile'><p class='stat-value tabular'>$($r.firstRejectedS)s</p><p class='stat-label'>from first publish to first rejection, with a $($r.quotaMb) MB quota</p></div>"
    }
    if ($byKey.never) {
        $c = $byKey.never.queues | Where-Object queue -eq "FANOUT-C"
        $tiles += "<div class='stat-tile status-bad'><p class='stat-value tabular'>$(Format-Count $c.discarded)</p><p class='stat-label'>messages FANOUT-C lost under <code>never</code>, with no signal to the publisher</p></div>"
    }
    if ($byKey.control) {
        $tiles += "<div class='stat-tile status-good'><p class='stat-value tabular'>$(Format-Count $byKey.control.rejected)</p><p class='stat-label'>rejections with the same absent subscriber, quota not reached</p></div>"
    }

    $scenarioSections = ($Results | ForEach-Object {
            $r = $_
            $rows = ($r.queues | ForEach-Object {
                    $missed = $r.sent - $_.distinct
                    $class = if ($missed -gt 0) { "violations-bad" } else { "violations-ok" }
                    "<tr><td>$(Encode $_.queue)</td><td>$(Format-Count $_.spooled)</td><td>$(Format-Count $_.distinct)</td><td class='$class'>$(Format-Count $missed)</td><td>$(Format-Count $_.discarded)</td><td>$([math]::Round($_.peakUsageBytes / 1MB, 2))</td></tr>"
                }) -join "`n"
            $reasons = @($r.reasons.PSObject.Properties | ForEach-Object { "<code>$(Encode $_.Name)</code>" }) -join " "
            $reasonLine = if ($reasons) { "<p class='chart-caption'>Rejection reason from the broker: $reasons</p>" } else { "" }
            @"
  <section class="scenario" data-scenario="$(Encode $r.key)">
    <h2>$(Encode $r.title)</h2>
    <p class="section-intro">FANOUT-C: $($r.quotaMb) MB quota, <code>reject-msg-to-sender-on-discard</code> = <code>$(Encode $r.discard)</code>. Publisher: $(Format-Count $r.sent) sent, $(Format-Count $r.acked) ACKed, $(Format-Count $r.rejected) rejected, $($r.sendFailures) send failures, $($r.noVerdict) without a verdict.</p>
    <p class="finding">$(Encode (Get-ScenarioFinding $r))</p>
    <div class="charts"></div>
    $reasonLine
    <div class="table-scroll"><table>
      <thead><tr><th>Queue</th><th>Accepted by queue</th><th>Handled by subscriber</th><th>Missed of $(Format-Count $r.sent) sent</th><th>Discarded (quota)</th><th>Peak spool MB</th></tr></thead>
      <tbody>
$rows
      </tbody>
    </table></div>
  </section>
"@
        }) -join "`n"

    $notes = [System.Collections.Generic.List[string]]::new()
    $notes.Add("<strong>Rejection is all-or-nothing across the fan-out.</strong> The setting belongs to the full queue, but a rejected message goes to none of the queues. The evidence is per message: each rejected sequence number is checked against every sequence each subscriber handled.")
    $notes.Add("<strong><code>never</code> contains the damage but hides it.</strong> Healthy queues keep flowing, and the full queue loses messages with no publisher-side signal. If you use it, alert on the broker's per-queue discard counter (<code>maxMsgSpoolUsageExceededDiscardedMsgCount</code>).")
    if ($byKey.control) {
        $c = $byKey.control.queues | Where-Object queue -eq "FANOUT-C"
        $mbPerSecond = ($c.peakUsageBytes / 1MB) / $byKey.control.subscriberCStartS
        $minutes = [math]::Round($byKey.control.quotaMb / $mbPerSecond / 60)
        $notes.Add("<strong>A larger quota buys time; it isn't a fix.</strong> The control backlog grew at about $([math]::Round($mbPerSecond, 2)) MB/s, so by extrapolation its $($byKey.control.quotaMb) MB quota would last roughly $minutes minutes at this rate. The useful questions are how long a subscriber can be down before the limit is reached, and whether an alert fires first.")
    }
    $notes.Add("<strong>Missed means this publisher doesn't retry,</strong> by design for this lab: it logs and continues. A retrying publisher could avoid the loss, but those messages would still reach nobody until the full queue had room.")
    $notes.Add("<strong>Limits of this run.</strong> One run per scenario on a local Docker broker at a fixed rate, with handlers doing no work. The small quota compresses time; the counts scale with rate and outage length and aren't predictions for any other system. VPN-level spool limits are out of scope.")
    $noteItems = ($notes | ForEach-Object { "<li>$_</li>" }) -join "`n      "

    $chartData = $Results | ForEach-Object {
        [pscustomobject]@{
            key = $_.key; quotaMb = $_.quotaMb; subscriberCStartS = $_.subscriberCStartS
            publisher = $_.publisher; samples = $_.samples
        }
    }
    $json = (@($chartData) | ConvertTo-Json -Depth 6 -Compress).Replace("<", "<")

    $html = [System.IO.File]::ReadAllText((Join-Path $PSScriptRoot "fanout-spool-quota-report.template.html")).
    Replace("@@STAMP@@", $Stamp).
    Replace("@@CSTART@@", "$([int]$first.subscriberCStartS)").
    Replace("@@TOPIC@@", $Topic).
    Replace("@@RATE@@", "$($first.ratePerSecond)").
    Replace("@@PAYLOAD@@", "$($first.payloadBytes)").
    Replace("@@DURATION@@", "$($first.durationSeconds)").
    Replace("@@DATE@@", (Get-Date -Format "yyyy-MM-dd")).
    Replace("@@TILES@@", $tiles).
    Replace("@@SCENARIOS@@", $scenarioSections).
    Replace("@@NOTES@@", $noteItems).
    Replace("@@JSON@@", $json)

    [System.IO.File]::WriteAllText($OutputPath, $html)
}

if ($RenderFromResults) {
    $saved = @(Get-Content $RenderFromResults -Raw | ConvertFrom-Json)
    $outputPath = Join-Path $ReportsDir "benchmark-fanout-spool-quota-$Stamp.html"
    New-FanoutReport -Results $saved -OutputPath $outputPath
    Write-Host "Report: $outputPath"
    exit 0
}

# --- Run ----------------------------------------------------------------------

try {
    Invoke-RestMethod -Uri "$SempBaseUrl/SEMP/v2/monitor/about/api" -Headers $AuthHeader | Out-Null
}
catch {
    Write-Error "Could not reach the broker's SEMP API at $SempBaseUrl. Is it running (./scripts/start-solace.ps1)?"
    exit 1
}

Write-Host "=== Building subscriber and fan-out publisher (Debug) ==="
dotnet build (Join-Path $SubscriberDir "messaging-lab.solace.subscriber.csproj") -c Debug | Out-Null
dotnet build (Join-Path $FanoutDir "messaging-lab.solace.fanout.csproj") -c Debug | Out-Null
$SubscriberDll = Join-Path $SubscriberDir "bin/Debug/net10.0/messaging-lab.solace.subscriber.dll"
$FanoutDll = Join-Path $FanoutDir "bin/Debug/net10.0/messaging-lab.solace.fanout.dll"
if (-not (Test-Path $SubscriberDll)) { Write-Error "Expected build output not found: $SubscriberDll"; exit 1 }
if (-not (Test-Path $FanoutDll)) { Write-Error "Expected build output not found: $FanoutDll"; exit 1 }

$Results = @()
try {
    foreach ($key in $Scenarios) { $Results += Invoke-Scenario $key }
}
finally {
    Remove-LabQueues
}

$ResultsJson = $Results | ConvertTo-Json -Depth 8 -Compress
Set-Content (Join-Path $WorkDir "results.json") $ResultsJson
Write-Host "`nRaw run data: $WorkDir"

foreach ($r in $Results) {
    Write-Host "`n--- $($r.key) ---"
    Write-Host "sent=$($r.sent) acked=$($r.acked) rejected=$($r.rejected) sendFailures=$($r.sendFailures) noVerdict=$($r.noVerdict) firstRejected=$($r.firstRejectedS)s lastRejected=$($r.lastRejectedS)s cStart=$($r.subscriberCStartS)s"
    $r.queues | Select-Object queue, handled, distinct, rejectedAndHandled, missed, spooled, discarded, peakUsageBytes | Format-Table | Out-String | Write-Host
    if ($r.reasons) { Write-Host "reasons: $($r.reasons | ConvertTo-Json -Compress)" }
}

$OutputPath = Join-Path $ReportsDir "benchmark-fanout-spool-quota-$Stamp.html"
New-FanoutReport -Results $Results -OutputPath $OutputPath
Write-Host "`nReport: $OutputPath"
