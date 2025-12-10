#!/bin/bash
# ==============================================================================
# FB Nova Data Sync - Azure Deployment Script (Bash)
# ==============================================================================
# 
# This script deploys the FB Nova sync tool as a containerized application
# to Azure Container Instances with scheduled execution capability.
#
# Usage:
#   ./deploy.sh
#   ./deploy.sh --resource-group rg-fbnova --location eastus
#
# ==============================================================================

set -e

# Default configuration
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-fbnova-sync}"
LOCATION="${LOCATION:-southafricanorth}"
ACR_NAME="${ACR_NAME:-acrfbnovasync}"
CONTAINER_NAME="${CONTAINER_NAME:-aci-fbnova-sync}"
IMAGE_NAME="${IMAGE_NAME:-fb-nova-sync}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Helper functions
step() { echo -e "\n${CYAN}▶ $1${NC}"; }
success() { echo -e "  ${GREEN}✓ $1${NC}"; }
warn() { echo -e "  ${YELLOW}⚠ $1${NC}"; }
error() { echo -e "  ${RED}✗ $1${NC}"; exit 1; }

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --resource-group|-g)
            RESOURCE_GROUP="$2"
            shift 2
            ;;
        --location|-l)
            LOCATION="$2"
            shift 2
            ;;
        --acr-name)
            ACR_NAME="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -g, --resource-group  Resource group name (default: rg-fbnova-sync)"
            echo "  -l, --location        Azure location (default: southafricanorth)"
            echo "  --acr-name            Container registry name (default: acrfbnovasync)"
            echo "  -h, --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "============================================================"
echo "FB NOVA - AZURE DEPLOYMENT SCRIPT"
echo "============================================================"

# Check prerequisites
step "Checking prerequisites..."

if ! command -v az &> /dev/null; then
    error "Azure CLI not found. Install from: https://aka.ms/installazurecli"
fi
success "Azure CLI installed"

if command -v docker &> /dev/null; then
    success "Docker installed"
    HAS_DOCKER=true
else
    warn "Docker not found. Will use Azure ACR build."
    HAS_DOCKER=false
fi

# Login to Azure
step "Checking Azure login..."
if az account show &> /dev/null; then
    ACCOUNT_NAME=$(az account show --query "name" -o tsv)
    USER_NAME=$(az account show --query "user.name" -o tsv)
    success "Logged in as: $USER_NAME"
    echo "  Subscription: $ACCOUNT_NAME"
else
    echo "  Opening browser for Azure login..."
    az login
fi

# Create Resource Group
step "Creating Resource Group: $RESOURCE_GROUP"
if az group exists --name "$RESOURCE_GROUP" | grep -q "true"; then
    success "Resource group already exists"
else
    az group create --name "$RESOURCE_GROUP" --location "$LOCATION" --output none
    success "Resource group created in $LOCATION"
fi

# Create Azure Container Registry
step "Creating Azure Container Registry: $ACR_NAME"
if az acr show --name "$ACR_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    success "Container Registry already exists"
else
    az acr create \
        --name "$ACR_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --sku Basic \
        --admin-enabled true \
        --output none
    success "Container Registry created"
fi

# Get ACR details
ACR_LOGIN_SERVER=$(az acr show --name "$ACR_NAME" --query "loginServer" -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$ACR_NAME" --query "passwords[0].value" -o tsv)
success "ACR Login Server: $ACR_LOGIN_SERVER"

# Build and push image
step "Building and pushing Docker image..."

if [ "$HAS_DOCKER" = true ]; then
    echo "  Building locally with Docker..."
    docker build -t "${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}" .
    
    echo "  Logging in to ACR..."
    az acr login --name "$ACR_NAME"
    
    echo "  Pushing to ACR..."
    docker push "${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}"
else
    echo "  Building in Azure with ACR Tasks..."
    az acr build \
        --registry "$ACR_NAME" \
        --image "${IMAGE_NAME}:${IMAGE_TAG}" \
        --file Dockerfile \
        .
fi
success "Image built and pushed: ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}"

# Environment variables prompt
step "Environment Variables Configuration"
echo "  The container needs the following environment variables:"
echo "    - STORAGE_CONNECTION_STRING"
echo "    - STORAGE_KEY"
echo "    - AZURE_CLIENT_ID (Service Principal)"
echo "    - AZURE_CLIENT_SECRET (Service Principal)"
echo ""
read -p "  Would you like to configure these now? (y/n): " CONFIGURE_NOW

if [ "$CONFIGURE_NOW" = "y" ] || [ "$CONFIGURE_NOW" = "Y" ]; then
    echo ""
    read -p "  STORAGE_CONNECTION_STRING: " STORAGE_CONN_STR
    read -p "  STORAGE_KEY: " STORAGE_KEY
    read -p "  AZURE_CLIENT_ID: " CLIENT_ID
    read -sp "  AZURE_CLIENT_SECRET: " CLIENT_SECRET
    echo ""
    
    # Create container instance
    step "Creating Azure Container Instance: $CONTAINER_NAME"
    az container create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$CONTAINER_NAME" \
        --image "${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}" \
        --registry-login-server "$ACR_LOGIN_SERVER" \
        --registry-username "$ACR_NAME" \
        --registry-password "$ACR_PASSWORD" \
        --restart-policy Never \
        --cpu 2 \
        --memory 4 \
        --secure-environment-variables \
            STORAGE_CONNECTION_STRING="$STORAGE_CONN_STR" \
            STORAGE_KEY="$STORAGE_KEY" \
            AZURE_CLIENT_ID="$CLIENT_ID" \
            AZURE_CLIENT_SECRET="$CLIENT_SECRET" \
        --output none
    
    success "Container Instance created"
else
    warn "Skipping container creation. Create manually with environment variables."
    echo ""
    echo "  Example command:"
    echo "  az container create \\"
    echo "    --resource-group $RESOURCE_GROUP \\"
    echo "    --name $CONTAINER_NAME \\"
    echo "    --image ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG} \\"
    echo "    --registry-login-server $ACR_LOGIN_SERVER \\"
    echo "    --registry-username $ACR_NAME \\"
    echo "    --registry-password <ACR_PASSWORD> \\"
    echo "    --restart-policy Never \\"
    echo "    --cpu 2 --memory 4 \\"
    echo "    --secure-environment-variables \\"
    echo "      STORAGE_CONNECTION_STRING=<YOUR_VALUE> \\"
    echo "      STORAGE_KEY=<YOUR_VALUE> \\"
    echo "      AZURE_CLIENT_ID=<YOUR_VALUE> \\"
    echo "      AZURE_CLIENT_SECRET=<YOUR_VALUE>"
fi

# Summary
echo ""
echo "============================================================"
echo "DEPLOYMENT SUMMARY"
echo "============================================================"
echo "  Resource Group:     $RESOURCE_GROUP"
echo "  Location:           $LOCATION"
echo "  Container Registry: $ACR_LOGIN_SERVER"
echo "  Image:              ${IMAGE_NAME}:${IMAGE_TAG}"
echo "  Container:          $CONTAINER_NAME"
echo ""
echo "NEXT STEPS:"
echo "  1. Set up Service Principal access to Synapse databases"
echo "  2. Create Logic App for scheduling (see logic-app-schedule.json)"
echo "  3. Test the container: az container start -g $RESOURCE_GROUP -n $CONTAINER_NAME"
echo "  4. View logs: az container logs -g $RESOURCE_GROUP -n $CONTAINER_NAME"
echo ""

