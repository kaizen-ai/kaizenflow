[
    {
        "cpu"                   : ${common_values.cpu}, 
        "dnsSearchDomains"      : ${jsonencode(common_values.dnsSearchDomains)},
        "dnsServers"            : ${jsonencode(common_values.dnsServers)},
        "dockerSecurityOptions" : ${jsonencode(common_values.dockerSecurityOptions)},
        "environment"           : ${jsonencode(environment_values)},
        "essential"             : ${common_values.essential}, 
        "image"                 : "${common_values.image}",
        "links"                 : ${jsonencode(common_values.links)},
        "logConfiguration"      : {
            "logDriver" : "${common_values.logDriver}", 
            "options"   : {
                "awslogs-group"         : "${common_values.awslogs_group}",
                "awslogs-region"        : "${common_values.awslogs_region}",
                "awslogs-stream-prefix" : "${common_values.awslogs_stream_prefix}"
            }
        },
        "mountPoints"           : [
            {
                "containerPath" : "${common_values.containerPath}",
                "readOnly"      : ${common_values.readOnly},
                "sourceVolume"  : "${common_values.sourceVolume}"
            }
        ],
        "name"                  : "${common_values.taskDefinitionName}",
        "portMappings"          : ${jsonencode(common_values.portMappings)},
        "secrets"               : ${jsonencode(secrets_values)},
        "systemControls"        : ${jsonencode(common_values.systemControls)},
        "volumesFrom"           : ${jsonencode(common_values.volumesFrom)}
    }
]
            
