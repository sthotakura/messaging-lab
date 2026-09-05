<#
.SYNOPSIS
    Stops the local Solace PubSub+ Docker container used for development.
#>

$ContainerName = "solace"

$status = docker inspect -f '{{.State.Status}}' $ContainerName 2>$null
if (-not $?) {
    Write-Error "Container '$ContainerName' does not exist."
    exit 1
}

if ($status -ne "running") {
    Write-Host "Container '$ContainerName' is not running."
    exit 0
}

docker stop $ContainerName
