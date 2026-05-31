param(
    [string[]]$Target = @("windows-amd64"),
    [string]$OutputDir = "dist",
    [string]$Package = "./cmd/etl",
    [string]$Name = "go-etl",
    [switch]$Clean,
    [switch]$All
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
$OriginalLocation = Get-Location

$supportedTargets = @{
    "windows-amd64" = @{ GOOS = "windows"; GOARCH = "amd64"; Ext = ".exe" }
    "windows-arm64" = @{ GOOS = "windows"; GOARCH = "arm64"; Ext = ".exe" }
    "linux-amd64"   = @{ GOOS = "linux";   GOARCH = "amd64"; Ext = "" }
    "linux-arm64"   = @{ GOOS = "linux";   GOARCH = "arm64"; Ext = "" }
    "darwin-amd64"  = @{ GOOS = "darwin";  GOARCH = "amd64"; Ext = "" }
    "darwin-arm64"  = @{ GOOS = "darwin";  GOARCH = "arm64"; Ext = "" }
}

if ($All) {
    $Target = @($supportedTargets.Keys | Sort-Object)
}

$resolvedOutputDir = $OutputDir
if (-not [System.IO.Path]::IsPathRooted($resolvedOutputDir)) {
    $resolvedOutputDir = Join-Path $RepoRoot $resolvedOutputDir
}

$resolvedPackage = $Package
if (-not [System.IO.Path]::IsPathRooted($resolvedPackage)) {
    $resolvedPackage = Join-Path $RepoRoot $resolvedPackage
}

try {
    Set-Location $RepoRoot

    if ($Clean -and (Test-Path -LiteralPath $resolvedOutputDir)) {
        Remove-Item -LiteralPath $resolvedOutputDir -Recurse -Force
    }

    New-Item -ItemType Directory -Force -Path $resolvedOutputDir | Out-Null

    foreach ($targetName in $Target) {
        if (-not $supportedTargets.ContainsKey($targetName)) {
            $valid = ($supportedTargets.Keys | Sort-Object) -join ", "
            throw "Unsupported target '$targetName'. Valid targets: $valid"
        }

        $targetSpec = $supportedTargets[$targetName]
        $goos = $targetSpec["GOOS"]
        $goarch = $targetSpec["GOARCH"]
        $ext = $targetSpec["Ext"]
        $fileName = "{0}-{1}{2}" -f $Name, $targetName, $ext
        $outFile = Join-Path $resolvedOutputDir $fileName

        Write-Host "Building $targetName -> $outFile"

        $env:GOOS = $goos
        $env:GOARCH = $goarch
        $env:CGO_ENABLED = "0"

        go build -trimpath -ldflags "-s -w" -o $outFile $resolvedPackage
    }
}
finally {
    Remove-Item Env:\GOOS -ErrorAction SilentlyContinue
    Remove-Item Env:\GOARCH -ErrorAction SilentlyContinue
    Remove-Item Env:\CGO_ENABLED -ErrorAction SilentlyContinue
    Set-Location $OriginalLocation
}

Write-Host "Build complete. Output: $resolvedOutputDir"
