{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowReadFromNATGateway",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::${bucket_name}/*",
            "Condition": {
                "IpAddress": {
                    "aws:SourceIp": "${eip_public_ip}"
                }
            }
        },
        {
            "Sid": "AllowReadFromVpc",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::${bucket_name}/*",
            "Condition": {
                "StringEquals": {
                    "aws:sourceVpc": "${vpc_id}"
                }
            }
        }
    ]
}