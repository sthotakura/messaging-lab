<#
.SYNOPSIS
    Runs messaging-lab.solace.loadgen, publishing test messages to the data-changed topic.

.PARAMETER Count
    Total messages to publish. Overrides LoadGen:Count in appsettings.json for this run.

.PARAMETER KeyCount
    Distinct OrderId keys to round-robin across. Overrides LoadGen:KeyCount for this run.

.EXAMPLE
    ./scripts/run-publisher.ps1 -Count 1000 -KeyCount 8
#>

param(
    [int]$Count,
    [int]$KeyCount
)

$ProjectPath = Join-Path $PSScriptRoot "../messaging-lab.solace.loadgen"

$RunArgs = @()
if ($PSBoundParameters.ContainsKey("Count")) { $RunArgs += "--LoadGen:Count"; $RunArgs += "$Count" }
if ($PSBoundParameters.ContainsKey("KeyCount")) { $RunArgs += "--LoadGen:KeyCount"; $RunArgs += "$KeyCount" }

# `dotnet run --project <path>` does NOT change the working directory to the project folder, so
# appsettings.json (loaded relative to the current directory) silently goes unfound unless we cd in first.
Push-Location $ProjectPath
try {
    dotnet run -- @RunArgs
}
finally {
    Pop-Location
}
