<#
.SYNOPSIS
    Starts the local Solace PubSub+ Docker container used for development.
#>

$ContainerName = "solace"

$status = docker inspect -f '{{.State.Status}}' $ContainerName 2>$null
if (-not $?) {
    Write-Error "Container '$ContainerName' does not exist. Create it first, e.g.:`n  docker run -d --name $ContainerName --shm-size=2g -p 8080:8080 -p 55555:55555 -e username_admin_globalaccesslevel=admin -e username_admin_password=admin solace/solace-pubsub-standard:latest"
    exit 1
}

if ($status -eq "running") {
    Write-Host "Container '$ContainerName' is already running."
    exit 0
}

docker start $ContainerName
