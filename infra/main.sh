#!/bin/bash

# Setting up some colors for status messages
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'  # No Color

set -euo pipefail

# Trap any unexpected errors
trap 'echo -e "${RED}Unexpected error occurred. Exiting.${NC}"' ERR

DEPLOY_DIR=${PWD}
OPENVPN_USERNAME="ansible"

# Ensure root permissions
if [ "$(id -u)" != "0" ]; then
    echo -e "${RED}This script must be run as root.${NC}"
    exit 1
fi

# Ensure the script is run from the root directory
if [[ ! -d "${DEPLOY_DIR}/terraform" || ! -d "${DEPLOY_DIR}/ansible" ]]; then
    echo -e "${RED}Please run the script from the project's root directory.${NC}"
    exit 1
fi

# Check if required packages are installed
REQUIRED_PACKAGES=("terraform" "ansible" "aws" "oathtool" "openvpn3" "jq" "awk")

for pkg in "${REQUIRED_PACKAGES[@]}"; do
    if ! command -v $pkg &> /dev/null; then
        echo -e "${RED}$pkg could not be found. Please install it first.${NC}"
        exit 1
    fi
done

function configure_vpn() {
    # Extract the TOTP secret key after the VPN client setup
    echo "Extracting TOTP secret and generating OTP..."
    sleep 3

    TOTP_SEED_PATH="/opt/crypto_vpn/${OPENVPN_USERNAME}/${OPENVPN_USERNAME}@vpn1.eu-north-1_embedded/${OPENVPN_USERNAME}@vpn1.eu-north-1-totp.seed"
    TOTP_SECRET=$(grep -oP 'Your new secret key is: \K[A-Z0-9]+' $TOTP_SEED_PATH)

    if [ -z "$TOTP_SECRET" ]; then
        echo -e "${RED}Failed to extract TOTP secret. Aborting.${NC}"
        exit 1
    fi

    OTP=$(oathtool --totp -b "$TOTP_SECRET")  # Generating OTP

    if [[ -z "$OTP" ]]; then
      echo -e "${RED}Failed to generate OTP. Aborting.${NC}"
      exit 1
    fi

    echo "Generated OTP: $OTP"

    # Establishing VPN connection using the generated OTP
    echo "Establishing VPN connection with OTP..."

    OVPN_FILE="/opt/crypto_vpn/${OPENVPN_USERNAME}/${OPENVPN_USERNAME}@vpn1.eu-north-1_embedded/${OPENVPN_USERNAME}@vpn1.eu-north-1-pki-embedded.ovpn"
    PASSWORD_FILE="/opt/crypto_vpn/${OPENVPN_USERNAME}/user_credentials/vpn1.eu-north-1_password.txt"
    PASSPHRASE_FILE="/opt/crypto_vpn/${OPENVPN_USERNAME}/user_credentials/vpn1.eu-north-1_pem_passphrase.txt"
    CREDENTIALS="/opt/crypto_vpn/${OPENVPN_USERNAME}/auth.cfg"

    # Fetch password and passphrase
    PASSWORD=$(cat $PASSWORD_FILE)
    PASSPHRASE=$(cat $PASSPHRASE_FILE)

    # Combine password and generated OTP
    COMBINED_PASSWORD="$PASSWORD$OTP"

    # Write credentials to auth.cfg
    echo "$OPENVPN_USERNAME" > $CREDENTIALS
    echo "$COMBINED_PASSWORD" >> $CREDENTIALS
    echo "$PASSPHRASE" >> $CREDENTIALS

    # Set permissions for auth.cfg
    sudo chown root:root $CREDENTIALS
    sudo chmod 600 $CREDENTIALS

    # Connect to the VPN server
    sudo cat $CREDENTIALS | openvpn3 session-start --config $OVPN_FILE

}

function deploy_infrastructure() {
    # Initializing bootstrap terraform
    echo "Initializing and applying bootstrap Terraform configurations..."
    cd "${DEPLOY_DIR}/terraform/bootstrap"
    terraform init
    terraform apply
    echo ""

    # Initialize and apply main terraform
    echo "Moving to main Terraform directory..."
    cd "${DEPLOY_DIR}/terraform"
    sleep 3
    terraform init
    echo "Applying main Terraform configurations..."
    terraform apply
    echo ""
    sleep 6

    # Capture the public IP of the VPN server from Terraform outputs
    VPN_PUBLIC_IP=$(terraform output -json vpn_server_public_ip | jq -r '.["VPN-terraform"]')

    # Capture the EFS DNS name from Terraform outputs
    EFS_DNS_NAME=$(terraform output -json efs_dns_name | jq -r .)

    # Capture the RDS endpoint from Terraform outputs
    RDS_ENDPOINT=$(terraform output -json rds_endpoint | jq -r . | awk -F: '{print $1}')

    # Update the hosts.ini file with the captured IP address
    sed -i "s/^vpn_host ansible_host=[0-9.]\+/vpn_host ansible_host=${VPN_PUBLIC_IP}/" "${DEPLOY_DIR}/ansible/inventory/hosts.ini"

    # Execute the Ansible playbook for VPN Server
    sleep 9
    echo "Running Ansible Playbook for VPN Server..."
    sleep 3
    cd "${DEPLOY_DIR}/ansible"
    ansible-playbook -i inventory/hosts.ini playbooks/vpn_server.yml -e "vpn_region=eu"

    # Execute the Ansible playbook for VPN Client
    echo "Running Ansible Playbook for VPN Client..."
    sleep 3
    cd "${DEPLOY_DIR}/ansible"
    sudo mkdir -p "/opt/crypto_vpn/${OPENVPN_USERNAME}/user_credentials"
    ansible-playbook -i inventory/hosts.ini playbooks/vpn_client.yml -e "openvpn_client_username=${OPENVPN_USERNAME}" -e "vpn_client_action=add" -e "vpn_region=eu" -e "openvpn_server_remote_host=${VPN_PUBLIC_IP}"

    # Extract TOTP secret and generate OTP - Establishing VPN connection using the generated OTP
    configure_vpn

    # Execute the Ansible playbook for Airflow
    echo "Running Ansible Playbook for Airflow..."
    sleep 3
    cd "${DEPLOY_DIR}/ansible"
    ansible-playbook -i inventory/hosts.ini playbooks/airflow.yml -e "github_user=${GITHUB_USER}" -e "github_pat=${GITHUB_PAT}" -e "deployment_env=${DEPLOYMENT_ENV}" -e "efs_dns_name=${EFS_DNS_NAME}"

    # Execute the Ansible playbook for RDS instance
    echo "Running Ansible Playbook for RDS instance..."
    ansible-playbook -i inventory/hosts.ini playbooks/rds_setup.yml -e "rds_endpoint=${RDS_ENDPOINT}"

    # Execute the Ansible playbook for Zabbix Server
    echo "Running Ansible Playbook for Zabbix Server..."
    ansible-playbook -i inventory/hosts.ini playbooks/zabbix_server.yml

    # Disconnect from the VPN Server
    echo "Disconnecting from the VPN Server..."
    openvpn3 session-manage --config $OVPN_FILE --disconnect

}

function destroy_infrastructure() {
    echo "Destroying infrastructure..."

    # Destroy main Terraform configuration
    cd "${DEPLOY_DIR}/terraform"
    terraform init
    terraform destroy

    # Destroy bootstrap Terraform configuration
    cd "${DEPLOY_DIR}/terraform/bootstrap"
    terraform init
    terraform destroy
}

# Main menu
echo "Please choose an option:"
echo "1. Deploy Infrastructure    - Deploy infrastructure and setup Airflow."
echo "2. Destroy Infrastructure   - Destroy the deployed infrastructure."
read -p "Enter your choice (1/2): " choice

case $choice in
    1)
        # Get Github credentials and deployment environment
        read -p "Enter your GitHub username: " GITHUB_USER
        [ -z "${GITHUB_USER}" ] && { echo -e "${RED}GitHub username is required.${NC}"; exit 1; }
        read -sp "Enter your GitHub personal access token (input will be hidden): " GITHUB_PAT
        echo ""
        [ -z "${GITHUB_PAT}" ] && { echo -e "${RED}GitHub personal access token is required.${NC}"; exit 1; }
        read -p "Enter the deployment environment (prod/preprod): " DEPLOYMENT_ENV
        [ -z "${DEPLOYMENT_ENV}" ] && { echo -e "${RED}Deployment environment is required.${NC}"; exit 1; }

        echo -e "${GREEN}Deploying Infrastructure...${NC}"
        deploy_infrastructure
        ;;
    2)
        read -rp "Are you sure you want to destroy the infrastructure? (yes/no) [no]: " confirmation
        confirmation="${confirmation:-no}"  # Default to 'no' if no input
        if [ "$confirmation" == "yes" ]; then
            printf "${RED}Destroying Infrastructure...${NC}\n"
            destroy_infrastructure
        else
            echo "Operation cancelled."
        fi
        ;;
    *)
        echo "Invalid option selected."
        ;;
esac
