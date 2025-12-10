<#
.SYNOPSIS
    Deploy FB Nova Data Sync to Azure Container Instances

.DESCRIPTION
    This script deploys the FB Nova sync tool as a containerized application
    to Azure Container Instances with scheduled execution via Logic Apps.

.PARAMETER ResourceGroup
    Azure Resource Group name (default: rg-fbnova-sync)

.PARAMETER Location
    Azure region (default: southafricanorth)

.PARAMETER AcrName
    Azure Container Registry name (default: acrfbnovasync)

.EXAMPLE
    .\deploy.ps1 -ResourceGroup "rg-fbnova-sync" -Location "southafricanorth"
#>

param(
    [string]$ResourceGroup = "rg-fbnova-sync",
    [string]$Location = "southafricanorth",
    [string]$AcrName = "acrfbnovasync",
    [string]$ContainerName = "aci-fbnova-sync",
    [string]$ImageName = "fb-nova-sync",
    [string]$ImageTag = "latest"
)

# Colors for output
function Write-Step { param($msg) Write-Host "`n▶ $msg" -ForegroundColor Cyan }
function Write-Success { param($msg) Write-Host "  ✓ $msg" -ForegroundColor Green }
function Write-Warn { param($msg) Write-Host "  ⚠ $msg" -ForegroundColor Yellow }
function Write-Err { param($msg) Write-Host "  ✗ $msg" -ForegroundColor Red }

Write-Host "=" * 60
Write-Host "FB NOVA - AZURE DEPLOYMENT SCRIPT"
Write-Host "=" * 60

# Check Azure CLI is installed
Write-Step "Checking prerequisites..."
try {
    $azVersion = az --version 2>$null | Select-Object -First 1
    Write-Success "Azure CLI installed: $azVersion"
} catch {
    Write-Err "Azure CLI not found. Install from: https://aka.ms/installazurecli"
    exit 1
}

# Check Docker is installed (for local builds)
try {
    $dockerVersion = docker --version 2>$null
    Write-Success "Docker installed: $dockerVersion"
    $hasDocker = $true
} catch {
    Write-Warn "Docker not found. Will use Azure ACR build instead."
    $hasDocker = $false
}

# Login to Azure
Write-Step "Logging in to Azure..."
$loginStatus = az account show 2>$null | ConvertFrom-Json
if ($loginStatus) {
    Write-Success "Already logged in as: $($loginStatus.user.name)"
    Write-Host "  Subscription: $($loginStatus.name)"
} else {
    Write-Host "  Opening browser for Azure login..."
    az login
}

# Create Resource Group
Write-Step "Creating Resource Group: $ResourceGroup"
$rgExists = az group exists --name $ResourceGroup
if ($rgExists -eq "true") {
    Write-Success "Resource group already exists"
} else {
    az group create --name $ResourceGroup --location $Location --output none
    Write-Success "Resource group created"
}

# Create Azure Container Registry
Write-Step "Creating Azure Container Registry: $AcrName"
$acrExists = az acr show --name $AcrName --resource-group $ResourceGroup 2>$null
if ($acrExists) {
    Write-Success "Container Registry already exists"
} else {
    az acr create `
        --name $AcrName `
        --resource-group $ResourceGroup `
        --sku Basic `
        --admin-enabled true `
        --output none
    Write-Success "Container Registry created"
}

# Get ACR credentials
Write-Step "Getting ACR credentials..."
$acrPassword = az acr credential show --name $AcrName --query "passwords[0].value" -o tsv
$acrLoginServer = az acr show --name $AcrName --query "loginServer" -o tsv
Write-Success "ACR Login Server: $acrLoginServer"

# Build and push image
Write-Step "Building and pushing Docker image..."
if ($hasDocker) {
    Write-Host "  Building locally with Docker..."
    docker build -t "${AcrName}.azurecr.io/${ImageName}:${ImageTag}" .
    
    Write-Host "  Logging in to ACR..."
    az acr login --name $AcrName
    
    Write-Host "  Pushing to ACR..."
    docker push "${AcrName}.azurecr.io/${ImageName}:${ImageTag}"
} else {
    Write-Host "  Building in Azure with ACR Tasks..."
    az acr build `
        --registry $AcrName `
        --image "${ImageName}:${ImageTag}" `
        --file Dockerfile `
        .
}
Write-Success "Image built and pushed: ${acrLoginServer}/${ImageName}:${ImageTag}"

# Prompt for environment variables
Write-Step "Environment Variables Configuration"
Write-Host "  The container needs the following environment variables:"
Write-Host "    - STORAGE_CONNECTION_STRING"
Write-Host "    - STORAGE_KEY"
Write-Host "    - AZURE_CLIENT_ID (Service Principal)"
Write-Host "    - AZURE_CLIENT_SECRET (Service Principal)"
Write-Host ""

$configureNow = Read-Host "  Would you like to configure these now? (y/n)"

if ($configureNow -eq "y") {
    Write-Host ""
    $storageConnStr = Read-Host "  STORAGE_CONNECTION_STRING"
    $storageKey = Read-Host "  STORAGE_KEY"
    $clientId = Read-Host "  AZURE_CLIENT_ID"
    $clientSecret = Read-Host "  AZURE_CLIENT_SECRET" -AsSecureString
    $clientSecretPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($clientSecret))
    
    # Create container instance
    Write-Step "Creating Azure Container Instance: $ContainerName"
    az container create `
        --resource-group $ResourceGroup `
        --name $ContainerName `
        --image "${acrLoginServer}/${ImageName}:${ImageTag}" `
        --registry-login-server $acrLoginServer `
        --registry-username $AcrName `
        --registry-password $acrPassword `
        --restart-policy Never `
        --cpu 2 `
        --memory 4 `
        --secure-environment-variables `
            STORAGE_CONNECTION_STRING="$storageConnStr" `
            STORAGE_KEY="$storageKey" `
            AZURE_CLIENT_ID="$clientId" `
            AZURE_CLIENT_SECRET="$clientSecretPlain" `
        --output none
    
    Write-Success "Container Instance created"
} else {
    Write-Warn "Skipping container creation. Create manually with environment variables."
    Write-Host ""
    Write-Host "  Example command:"
    Write-Host @"
  az container create ``
    --resource-group $ResourceGroup ``
    --name $ContainerName ``
    --image ${acrLoginServer}/${ImageName}:${ImageTag} ``
    --registry-login-server $acrLoginServer ``
    --registry-username $AcrName ``
    --registry-password <ACR_PASSWORD> ``
    --restart-policy Never ``
    --cpu 2 --memory 4 ``
    --secure-environment-variables ``
      STORAGE_CONNECTION_STRING=<YOUR_VALUE> ``
      STORAGE_KEY=<YOUR_VALUE> ``
      AZURE_CLIENT_ID=<YOUR_VALUE> ``
      AZURE_CLIENT_SECRET=<YOUR_VALUE>
"@
}

# Summary
Write-Host ""
Write-Host "=" * 60
Write-Host "DEPLOYMENT SUMMARY"
Write-Host "=" * 60
Write-Host "  Resource Group:    $ResourceGroup"
Write-Host "  Location:          $Location"
Write-Host "  Container Registry: $acrLoginServer"
Write-Host "  Image:             ${ImageName}:${ImageTag}"
Write-Host "  Container:         $ContainerName"
Write-Host ""
Write-Host "NEXT STEPS:"
Write-Host "  1. Set up Service Principal access to Synapse databases"
Write-Host "  2. Create Logic App for scheduling (see logic-app-schedule.json)"
Write-Host "  3. Test the container: az container start -g $ResourceGroup -n $ContainerName"
Write-Host "  4. View logs: az container logs -g $ResourceGroup -n $ContainerName"
Write-Host ""

