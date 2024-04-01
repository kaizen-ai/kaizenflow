variable "stage" {
  description = "affects only the resource name, specifies which stage are the resources created in"
  type = string
}

variable "tags_cluster" {
  description = "tags"
  type = map(string)
  default = {}
}

variable "tags_all_cluster" {
  description = "tags all"
  type = map(string)
  default = {}
}

variable "containerInsights_value" {
  description = "enable or disable container insights"
  type = string
}

variable "cpu_td" {
  description = "number of CPU units, 1024 = 1 vCPU core"
  type = string
  default = "256"
}

variable "execution_role_arn_td" {
  description = "arn of the execution role"
  type = string
}

variable "ipc_mode_td" {
  description = "IPC resource namespace to be used for the containers in the task."
  type = string
  default = null
}

variable "memory_td" {
  description = "memory to be configured, in mebibytes"
  type = string
  default = "512"
}

variable "network_mode_td" {
  description = "Docker networking mode to use for the containers in the task."
  type = string
  default = "awsvpc"
}

variable "pid_mode_td" {
  description = "Process namespace to use for the containers in the task."
  type = string
  default = null
}

variable "requires_compatibilities_td" {
  description = "Set of launch types required by the task."
  type = set(string)
  default = ["EC2", "FARGATE"]
}

variable "skip_destroy_td" {
  description = "Whether to retain the old revision when the resource is destroyed or replacement is necessary."
  type = bool
  default = null
}

variable "tags_td" {
  description = "tags"
  type = map(string)
  default = {}
}

variable "tags_all_td" {
  description = "tags all"
  type = map(string)
  default = {}
}

variable "task_role_arn_td" {
  description = "arn of the task role"
  type = string
}

variable "track_latest_td" {
  description = "track latest"
  type = bool
  default = false
}

variable "host_path_td_efs" {
  description = "Path on the host container instance that is presented to the container. If not set, ECS will create a nonpersistent data volume that starts empty and is deleted after the task has finished."
  type = string
}

variable "name_td_efs" {
  description = "name of the efs volume"
  type = string
}

variable "file_system_id_td_efs" {
  description = "ID of the EFS File System"
  type = string
}

variable "root_directory_td_efs" {
  description = "Directory within the Amazon EFS file system to mount as the root directory inside the host. If this parameter is omitted, the root of the Amazon EFS volume will be used."
  type = string
}

variable "transit_encryption_td_efs" {
  description = "Whether or not to enable encryption for Amazon EFS data in transit between the Amazon ECS host and the Amazon EFS server."
    type = string
}

variable "transit_encryption_port_td_efs" {
  description = "Port to use for transit encryption."
  type = number
}

variable "access_point_id_td_efs" {
  description = "Access point ID to use."
  type = string
}

variable "iam_td_efs" {
  description = "Whether or not to use the Amazon ECS task IAM role defined in a task definition when mounting the Amazon EFS file system."
  type = string
}


######_json_variables_below_######


variable "cpu_cd" {
  description = "cpu value for the json code (different from the cpu variable)"
  type = number
  default = 0
}

variable "dnsSearchDomains" {
  description = "JSON DNS search domains"
  type = list
  default = []
}

variable "dnsServers" {
  description = "JSON DNS servers"
  type = list
  default = []
}

variable "dockerSecurityOptions" {
  description = "JSON docker security options"
  type = list
  default = []
}

variable "essential" {
  description = "container definition - essential"
  type = bool
  default = true
}

variable "image" {
  description = "container definition - image"
  type = string
}

variable "links" {
  description = "container definition - links"
  type = list
}

variable "logDriver" {
  description = "log configuration - logDriver"
  type = string
}

variable "awslogsGroup" {
  description = "log configuration - options - awslogs-group"
  type = string
}

variable "awslogsRegion" {
  description = "log configuration - options - awslogs-region"
  type = string
}

variable "awslogsStreamPrefix" {
  description = "log configuration - options - awslogs-stream-prefix"
  type = string
}

variable "containerPath" {
  description = "mountpoints - containerPath"
  type = string
}

variable "readOnly" {
  description = "mountpoints - readonly"
  type = bool
}

variable "sourceVolume" {
  description = "mountpoints - sourcevolume"
  type = string
}

variable "taskDefinitionName" {
  description = "name of the task definition"
  type = string
}

variable "portMappings" {
  description = "portmappings"
  type = list
}

variable "systemControls" {
  description = "systemcontrols"
  type = list
}

variable "volumesFrom" {
  description = "volumesfrom"
  type = list
}


######_json_environment_variables_#####


variable "AM_AWS_S3_BUCKET" {
  type = string
}

variable "AM_ECR_BASE_PATH" {
  type = string
}

variable "AM_ENABLE_DIND" {
  type = string
}

variable "CK_AWS_DEFAULT_REGION" {
  type = string
}

variable "CK_AWS_S3_BUCKET" {
  type = string
}

variable "CK_ECR_BASE_PATH" {
  type = string
}

variable "POSTGRES_DB" {
  type = string
}

variable "POSTGRES_HOST" {
  type = string
}

variable "POSTGRES_PORT" {
  type = string
}


#####_json_secrets_variables_#####


variable "POSTGRES_PASSWORD" {
  type = string
}

variable "POSTGRES_USER" {
  type = string
}
