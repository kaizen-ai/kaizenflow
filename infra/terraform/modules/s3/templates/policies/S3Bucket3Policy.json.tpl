{
    "Version": "2012-10-17",
    "Id": "S3-Console-Auto-Gen-Policy-1661357728017",
    "Statement": [
        {
            "Sid": "S3PolicyStmt-DO-NOT-MODIFY-1661357727846",
            "Effect": "Allow",
            "Principal": {
                "Service": "logging.s3.amazonaws.com"
            },
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::${bucket_name}/*"
        }
    ]
}