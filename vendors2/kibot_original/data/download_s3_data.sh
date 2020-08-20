#!/bin/bash -xe

DIR="eod2019cont"
aws s3 cp s3://$DIR all_primer_data/$DIR --recursive

DIR="eodcommods"
aws s3 cp s3://$DIR all_primer_data/$DIR --recursive

DIR="eodfutures"
aws s3 cp s3://$DIR all_primer_data/$DIR --recursive
#minutebars

DIR="minutebars2019"
aws s3 cp s3://$DIR all_primer_data/$DIR --recursive

DIR="minutecompressed"
aws s3 cp s3://$DIR all_primer_data/$DIR --recursive

DIR="minuteunstiched"
aws s3 cp s3://$DIR all_primer_data/$DIR --recursive

DIR="tickdata2019"
aws s3 cp s3://$DIR all_primer_data/$DIR --recursive

DIR="top10futures"
aws s3 cp s3://$DIR all_primer_data/$DIR --recursive
