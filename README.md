![Rust](https://github.com/ashrithr/aws-nuke/workflows/Rust/badge.svg?branch=master)

# AWS Resource Cleaner

Cleans up AWS resources based on configurable Rules. This project is a WIP.

## Supported Services

* EC2
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up based on Idle Instances based on CloudWatch metrics
    - Clean up based on Security Group rules
    - Clean up idle EBS volumes
* RDS
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up based in Idle Instances based on CloudWatch metrics
* RDS Aurora
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up based in Idle Instances based on CloudWatch metrics
* S3
    - Clean up based on bucket naming prefix

## Configuration

aws-nuke requires a configuration file to operate with, sample configuration is located for reference here: `examples/config/sample.toml`.

Make a copy of the sample configuration and make changes as needed based on the comments provided in the sample configuration file.

```
cp examples/config/sample.toml config.toml
```

## Build and Running

aws-nuke can be built using the following command:

```
cargo build --release
```

Once built, run using the following command:

```
./target/release/aws-nuke -C config.toml -vvv
```

To get help:

```
./target/release/aws-nuke -h
```

## Docker

aws-nuke can be built using Docker:

```
docker build -t aws-nuke .
```

```
docker run \
-v "$PWD/examples/configs":/configs \
-e AWS_ACCESS_KEY_ID=REPLACE_WITH_ACCESS_KEY \
-e AWS_SECRET_ACCESS_KEY=REPLACE_WITH_SECRET_KEY \
aws-nuke -C /configs/sample.toml -vvv
```