param(
    [string]$Url = "http://localhost:8080/trends?limit=10",
    [int]$Connections = 200,
    [string]$Duration = "30s"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command hey -ErrorAction SilentlyContinue)) {
    Write-Error "hey is not installed. Install it with: go install github.com/rakyll/hey@latest"
}

hey -z $Duration -c $Connections $Url
