#!/usr/bin/env bash
#
# A simple wrapper to execute nuker as a cron job
#
# Sample job to execute this script every 6 hours
# 0 */6 * * * /path/to/this/run.sh /var/tmp/nuker.toml AWS_ACCESS_KEY AWS_SECRET_KEY v1.0.0 > /var/tmp/nuker_`/bin/date +\%s`.log 2>&1

if [[ $# -ne 4 ]];
then
  echo >&2 "[Usage]: $(basename $0) /path/to/config.toml /path/to/.aws v1.0.0 [email_address]"
  echo -e >&2 "\t1. Absolute path to the nuker configuration file"
  echo -e >&2 "\t2. AWS Access Key ID"
  echo -e >&2 "\t3. AWS Secret Access Key" 
  echo -e >&2 "\t4. Nuker docker image version to fetch and execute."
  echo -e >&2 "\t5. (Optional) Email address to send the log file to. Expects mailx to be configured."

  exit 1
fi

start_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
log_file=/var/tmp/nuker-${start_time}.log
config_file=$1
aws_access_key_id=$2
aws_secret_access_key=$3
image_version=$4

echo "Logging to ${log_file}"

# Check if docker is available
command -v docker >/dev/null 2>&1 || {
  echo >&2 "'docker' command not found"
  exit 2
}

regions=( ap-east-1 ap-northeast-1 ap-northeast-2 ap-south-1 ap-southeast-1 ap-southeast-2 ca-central-1 eu-central-1 eu-north-1 eu-west-1 eu-west-2 eu-west-3 me-south-1 sa-east-1 us-east-1 us-east-2 us-west-1 us-west-2 )

for region in "${regions[@]}"
do
    docker run --rm \
    -v ${config_file}:/home/nuker/nuker.toml \
    -e AWS_ACCESS_KEY_ID=${aws_access_key_id} \
    -e AWS_SECRET_ACCESS_KEY=${aws_secret_access_key} \
    ashrithr/nuker:${image_version} \
    --config /home/nuker/nuker.toml \
    --region ${region} \
    --exclude s3 \
    --no-dry-run \
    --force \
    -vvvv
done

if [[ $# -eq 5 ]]; then
  command -v mailx >/dev/null 2>&1 || {
    echo >&2 "'mailx' command not found, skipping send mail"
    exit 2
  }

  mailx -s "Nuker Run Log - ${start_time}" -a $log_file $5
fi