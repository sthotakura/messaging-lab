<#
.SYNOPSIS
    Runs messaging-lab.solace.subscriber, consuming from the CHANGED queue until Ctrl+C.

.PARAMETER Concurrent
    Forces UseConcurrentSubscriber to true for this run (binds SolaceConcurrentSubscriber<T>).

.PARAMETER Sequential
    Forces UseConcurrentSubscriber to false for this run (binds SolaceSequentialSubscriber<T>).

.PARAMETER Concurrency
    Worker/lane count for the concurrent subscriber. Overrides Subscriber:Concurrency for this run.

.EXAMPLE
    ./scripts/run-subscriber.ps1 -Concurrent -Concurrency 8

.EXAMPLE
    ./scripts/run-subscriber.ps1 -Sequential
#>

param(
    [switch]$Concurrent,
    [switch]$Sequential,
    [int]$Concurrency
)

if ($Concurrent -and $Sequential) {
    Write-Error "Specify at most one of -Concurrent / -Sequential."
    exit 1
}

$ProjectPath = Join-Path $PSScriptRoot "../messaging-lab.solace.subscriber"

$RunArgs = @()
if ($Concurrent) { $RunArgs += "--Subscriber:UseConcurrentSubscriber"; $RunArgs += "true" }
if ($Sequential) { $RunArgs += "--Subscriber:UseConcurrentSubscriber"; $RunArgs += "false" }
if ($PSBoundParameters.ContainsKey("Concurrency")) { $RunArgs += "--Subscriber:Concurrency"; $RunArgs += "$Concurrency" }

# `dotnet run --project <path>` does NOT change the working directory to the project folder, so
# appsettings.json (loaded relative to the current directory) silently goes unfound unless we cd in first.
Push-Location $ProjectPath
try {
    dotnet run -- @RunArgs
}
finally {
    Pop-Location
}
