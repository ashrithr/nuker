![Rust](https://github.com/ashrithr/nuker/workflows/Rust/badge.svg?branch=master)

# AWS Resource Cleaner

Cleans up AWS resources based on configurable Rules. This project is a WIP.

## Supported Services

* EC2
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up Idle Instances based on CloudWatch metrics
    - Clean up based on Security Group rules
    - Clean up idle EBS volumes
* RDS
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up based in Idle Instances based on CloudWatch metrics
* RDS Aurora
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up Idle Instances based on CloudWatch metrics
* Redshift
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up Idle Instances based on Cloudwatch metrics
* S3
    - Clean up based on bucket naming prefix
* EMR
    - Clean up based on Tags
    - Clean up based on allowed Instance types
    - Clean up Idle Instances based on Cloudwatch metrics
    - Clean up based on Security Group rules

## Configuration

nuker requires a configuration file to operate with, sample configuration is located for reference here: `examples/config/sample.toml`.

Make a copy of the sample configuration and make changes as needed based on the comments provided in the sample configuration file.

```
cp examples/config/sample.toml config.toml
```

## Build and Running

nuker can be built using the following command:

```
cargo build --release
```

Once built, run using the following command:

```
./target/release/nuker --config examples/configs/sample.toml \
--profile default \
--region us-east-1 \
--region us-east-2 \
-vvv
```

To get help:

```
./target/release/nuker -h
```

## Docker

nuker can be built using Docker:

```
docker build -t nuker .
```

```
docker run --rm -it \
-v "$PWD/examples/configs/sample.toml":/home/nuker/config.toml \
-e AWS_ACCESS_KEY_ID=REPLACE_WITH_ACCESS_KEY \
-e AWS_SECRET_ACCESS_KEY=REPLACE_WITH_SECRET_KEY \
ashrithr/nuker:latest \
--config /home/nuker/config.toml
```

or

```
docker run --rm -it \
-v "$PWD/examples/configs/sample.toml":/home/nuker/config.toml \
-v "$HOME/.aws":/home/nuker/.aws \
ashrithr/nuker:latest \
--profile default \
--config /home/nuker/config.toml
```