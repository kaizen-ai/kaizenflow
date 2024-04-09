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
if [[ ! -d "${DEPLOY_DIR}/terraform" || ! -d "${DEPLOY_DIR}/ansible" || ! -d "${DEPLOY_DIR}/kubernetes" ]]; then
    echo -e "${RED}Please run the script from the project's root directory.${NC}"
    exit 1
fi

# Check if required packages are installed
REQUIRED_PACKAGES=("terraform" "ansible" "aws" "oathtool" "openvpn3" "jq" "awk" "kubectl" "helm" "eksctl")

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

function deploy_ec2_infrastructure() {
    # Initializing bootstrap terraform
    echo "Initializing and applying bootstrap Terraform configurations..."
    cd "${DEPLOY_DIR}/terraform/bootstrap"
    terraform init
    terraform apply
    echo ""

    # Initialize and apply terraform
    echo "Moving to main Terraform directory..."
    cd "${DEPLOY_DIR}/terraform"
    sleep 3
    terraform init
    echo "Applying Terraform configurations..."
    terraform apply -target=module.vpc -target=module.ec2 -target=module.rds -target=module.efs -target=module.iam -target=module.aws_backup -target=module.s3
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

function deploy_k8s_infrastructure() {
    # Initializing bootstrap terraform
    echo "Initializing and applying bootstrap Terraform configurations..."
    cd "${DEPLOY_DIR}/terraform/bootstrap"
    terraform init
    terraform apply
    echo ""

    # Initialize and apply terraform
    echo "Moving to main Terraform directory..."
    cd "${DEPLOY_DIR}/terraform"
    sleep 3
    terraform init
    terraform apply -target=module.vpc -target=module.iam -target=module.eks -target=module.rds -target=module.efs
    echo ""
    sleep 6

    # Configure the kubeconfig for the cluster communication
    echo "Configuring the kubeconfig for the cluster communication..."
    aws eks --region ${AWS_REGION} --profile ${AWSCLI_PROFILE} update-kubeconfig --name ${CLUSTER_NAME}
    echo ""
    sleep 3

    # Capture the EFS DNS name from Terraform outputs
    EFS_DNS_NAME=$(terraform output -json efs_dns_name | jq -r .)

    echo "Moving to main K8s directory..."
    cd "${DEPLOY_DIR}/kubernetes"
    K8S_TEMPLATES_DIR="${DEPLOY_DIR}/kubernetes/airflow/templates"
    echo ""
    sleep 3

    # Execute Kubernetes configurations
    echo "Associating IAM OIDC provider with the EKS cluster..."
    eksctl utils associate-iam-oidc-provider --cluster ${CLUSTER_NAME} --profile ${AWSCLI_PROFILE} --region ${AWS_REGION} --approve
    echo ""
    sleep 3

    echo "Creating IAM service account for EFS CSI driver..."
    eksctl create iamserviceaccount --region ${AWS_REGION} --profile ${AWSCLI_PROFILE} --name efs-csi-controller-sa --namespace kube-system --cluster ${CLUSTER_NAME} --role-name AmazonEKS_EFS_CSI_DriverRole --role-only --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEFSCSIDriverPolicy --approve
    echo ""
    sleep 6

    echo "Updating IAM role trust policy for EFS CSI driver..."
    TRUST_POLICY=$(aws iam --region ${AWS_REGION} --profile ${AWSCLI_PROFILE} --output json get-role --role-name AmazonEKS_EFS_CSI_DriverRole --query 'Role.AssumeRolePolicyDocument' | sed -e 's/efs-csi-controller-sa/efs-csi-*/' -e 's/StringEquals/StringLike/')
    aws iam --region ${AWS_REGION} --profile ${AWSCLI_PROFILE} update-assume-role-policy --role-name AmazonEKS_EFS_CSI_DriverRole --policy-document "$TRUST_POLICY"
    echo ""
    sleep 3

    echo "Creating 'airflow' namespace..."
    kubectl create namespace airflow
    echo ""
    sleep 1

    echo "Applying EFS storage class configurations..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/pvcs/efs-storageclass.yaml" -n airflow
    echo ""
    sleep 3

    echo "Updating Persistent Volume YAML files with EFS DNS Name..."
    # Replace the volumeHandle with the actual EFS filesystem ID for DAGs
    sed -i "s|volumeHandle:.*|volumeHandle: ${EFS_DNS_NAME}:/airflow/dags|" "${K8S_TEMPLATES_DIR}/pvcs/dags-persistent-volume.yaml"
    # Replace the volumeHandle with the actual EFS filesystem ID for Logs
    sed -i "s|volumeHandle:.*|volumeHandle: ${EFS_DNS_NAME}:/airflow/logs|" "${K8S_TEMPLATES_DIR}/pvcs/logs-persistent-volume.yaml"
    echo ""
    sleep 1

    echo "Applying persistent volume and claims for airflow logs and dags storage..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/pvcs/logs-persistent-volume.yaml" -n airflow
    kubectl apply -f "$K8S_TEMPLATES_DIR/pvcs/logs-persistent-volume-claim.yaml" -n airflow
    kubectl apply -f "$K8S_TEMPLATES_DIR/pvcs/dags-persistent-volume.yaml" -n airflow
    kubectl apply -f "$K8S_TEMPLATES_DIR/pvcs/dags-persistent-volume-claim.yaml" -n airflow
    echo ""
    sleep 1

    echo "Applying ConfigMap for Airflow configurations..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/configmaps/configmap.yaml" -n airflow
    echo ""
    sleep 1

    echo "Creating IAM service account for External Secrets Manager (ESO)..."
    eksctl create iamserviceaccount --region ${AWS_REGION} --profile ${AWSCLI_PROFILE} --name airflow-eso-serviceaccount --namespace airflow --cluster ${CLUSTER_NAME} --role-name AmazonEKS_ESO_SecretsManagerRole --attach-policy-arn arn:aws:iam::623860924167:policy/secretsManagerReadOnly --approve
    echo ""
    sleep 3

    echo "Installing External Secrets from the Helm chart..."
    helm repo add external-secrets https://charts.external-secrets.io
    helm install external-secrets external-secrets/external-secrets -n external-secrets --create-namespace
    echo ""
    sleep 6

    echo "Applying ESO secret store configuration..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/secrets/eso-secretstore.yaml" -n airflow
    echo ""
    sleep 3

    echo "Applying secrets configuration..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/secrets/secrets.yaml" -n airflow
    echo ""
    sleep 1

    echo "Applying scheduler service account..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/scheduler/scheduler-serviceaccount.yaml" -n airflow
    echo ""
    sleep 1

    echo "Applying webserver service account..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/webserver/webserver-serviceaccount.yaml" -n airflow
    echo ""
    sleep 1

    echo "Applying pod launcher role configuration..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/rbac/pod-launcher-role.yaml" -n airflow
    echo ""
    sleep 1

    echo "Applying pod launcher role binding configuration..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/rbac/pod-launcher-rolebinding.yaml" -n airflow
    echo ""
    sleep 1

    echo "Applying pod log reader role configuration..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/rbac/pod-log-reader-role.yaml" -n airflow
    echo ""
    sleep 1

    echo "Applying pod log reader role binding configuration..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/rbac/pod-log-reader-rolebinding.yaml" -n airflow
    echo ""
    sleep 1

    echo "Deploying Airflow Scheduler..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/scheduler/scheduler-deployment.yaml" -n airflow
    echo ""
    sleep 9

    echo "Deploying Airflow Webserver..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/webserver/webserver-deployment.yaml" -n airflow
    echo ""
    sleep 9

    echo "Deploying Airflow Webserver service configuration..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/services/webserver-service.yaml" -n airflow
    echo ""
    sleep 9

    echo "Applying scheduler Horizontal Pod Autoscaler (HPA) configuration..."
    kubectl apply -f "$K8S_TEMPLATES_DIR/hpas/scheduler-hpa.yaml" -n airflow
    echo ""
    sleep 3

    echo -e "${GREEN}Kubernetes configurations applied successfully.${NC}"
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
echo "1. Deploy EC2 Infrastructure    - Deploy EC2 based infrastructure and setup Airflow."
echo "2. Deploy K8s Infrastructure    - Deploy EKS based infrastructure and setup Airflow."
echo "3. Destroy Infrastructure       - Destroy the deployed infrastructure."
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

        echo -e "${GREEN}Deploying EC2 Infrastructure...${NC}"
        deploy_ec2_infrastructure
        ;;
    2)
        # Collect AWS Region, AWS CLI Profile, and Cluster Name from the user
        read -p "Enter the AWS Region: " AWS_REGION
        [ -z "${AWS_REGION}" ] && { echo -e "${RED}AWS Region is required.${NC}"; exit 1; }
        read -p "Enter the AWS CLI Profile: " AWSCLI_PROFILE
        [ -z "${AWSCLI_PROFILE}" ] && { echo -e "${RED}AWS CLI Profile is required.${NC}"; exit 1; }
        read -p "Enter the Cluster Name: " CLUSTER_NAME
        [ -z "${CLUSTER_NAME}" ] && { echo -e "${RED}Cluster Name is required.${NC}"; exit 1; }

        echo -e "${GREEN}Deploying K8s Infrastructure...${NC}"
        deploy_k8s_infrastructure
        ;;
    3)
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
