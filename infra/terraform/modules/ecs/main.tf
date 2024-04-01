locals {
  common_values = {
    cpu                     = var.cpu_cd
    dnsSearchDomains        = var.dnsSearchDomains
    dnsServers              = var.dnsServers
    dockerSecurityOptions   = var.dockerSecurityOptions
    essential               = var.essential
    image                   = var.image
    links                   = var.links
    logDriver               = var.logDriver
    awslogs_group           = var.awslogsGroup
    awslogs_region          = var.awslogsRegion
    awslogs_stream_prefix   = var.awslogsStreamPrefix
    containerPath           = var.containerPath
    readOnly                = var.readOnly
    sourceVolume            = var.sourceVolume
    taskDefinitionName      = var.taskDefinitionName
    portMappings            = var.portMappings
    systemControls          = var.systemControls
    volumesFrom             = var.volumesFrom
  }
  environment_values = [
    {name = "AM_AWS_S3_BUCKET",         value = var.AM_AWS_S3_BUCKET},
    {name = "AM_ECR_BASE_PATH",         value = var.AM_ECR_BASE_PATH},
    {name = "AM_ENABLE_DIND",           value = var.AM_ENABLE_DIND},
    {name = "CK_AWS_DEFAULT_REGION",    value = var.CK_AWS_DEFAULT_REGION},
    {name = "CK_AWS_S3_BUCKET",         value = var.CK_AWS_S3_BUCKET},
    {name = "CK_ECR_BASE_PATH",         value = var.CK_ECR_BASE_PATH},
    {name = "POSTGRES_DB",              value = var.POSTGRES_DB},
    {name = "POSTGRES_HOST",            value = var.POSTGRES_HOST},
    {name = "POSTGRES_PORT",            value = var.POSTGRES_PORT}
  ]
  secrets_values = [
    { name = "POSTGRES_PASSWORD",   valueFrom = var.POSTGRES_PASSWORD},
    { name = "POSTGRES_USER",       valueFrom = var.POSTGRES_USER}
  ]
}


resource "aws_ecs_cluster" "ECSCluster" {
  name     = "kaizen-${var.stage}-cluster"
  tags     = var.tags_cluster
  tags_all = var.tags_all_cluster
  setting {
    name  = "containerInsights" # leaving this hardcoded as it is the only valid value
    value = var.containerInsights_value
  }
}


resource "aws_ecs_task_definition" "ECSTaskDefinition" {
  container_definitions    = templatefile("${path.module}/templates/ecs.json.tpl", {
    common_values       = local.common_values,
    environment_values  = local.environment_values,
    secrets_values      = local.secrets_values
})
   
  cpu                      = var.cpu_td
  execution_role_arn       = var.execution_role_arn_td
  family                   = "cmamp-${var.stage}"   # name of the task definition
  ipc_mode                 = var.ipc_mode_td
  memory                   = var.memory_td
  network_mode             = var.network_mode_td
  pid_mode                 = var.pid_mode_td
  requires_compatibilities = var.requires_compatibilities_td
  skip_destroy             = var.skip_destroy_td
  tags                     = var.tags_td
  tags_all                 = var.tags_all_td
  task_role_arn            = var.task_role_arn_td
  #track_latest             = var.track_latest_td  #version 5.37.0 and higher is needed for this to work
  volume {
    host_path = var.host_path_td_efs
    name      = var.name_td_efs
    efs_volume_configuration {
      file_system_id            = var.file_system_id_td_efs
      root_directory            = var.root_directory_td_efs
      transit_encryption        = var.transit_encryption_td_efs
      transit_encryption_port   = var.transit_encryption_port_td_efs
      authorization_config {
        access_point_id = var.access_point_id_td_efs
        iam             = var.iam_td_efs
      }
    }
  }
}