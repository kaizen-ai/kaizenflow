{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:InitiateReplication"
            ],
            "Resource": "arn:aws:s3:::tf-cryptokaizen-data/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::tf-cryptokaizen-data-backup/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::tf-cryptokaizen-data-test/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetReplicationConfiguration",
                "s3:PutInventoryConfiguration"
            ],
            "Resource": "arn:aws:s3:::tf-cryptokaizen-data"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::tf-cryptokaizen-data-backup/*"
        }
    ]
}