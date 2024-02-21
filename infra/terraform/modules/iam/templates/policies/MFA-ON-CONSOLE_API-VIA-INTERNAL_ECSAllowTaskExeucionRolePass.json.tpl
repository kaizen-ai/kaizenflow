{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowFromDevNoMFA",
            "Effect": "Allow",
            "Action": [
                "iam:GetRole",
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::*:role/ecsTaskExecutionRole",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": [
                        "eu-north-1",
                        "us-east-1"
                    ]
                },
                "IpAddress": {
                    "aws:SourceIp": [
                        "${eip_public_ip}"
                    ]
                }
            }
        },
        {
            "Sid": "AllowFromEverywhereWithMFA",
            "Effect": "Allow",
            "Action": [
                "iam:GetRole",
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::*:role/ecsTaskExecutionRole",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": [
                        "eu-north-1",
                        "us-east-1"
                    ]
                },
                "Bool": {
                    "aws:MultiFactorAuthPresent": "true"
                }
            }
        }
    ]
}