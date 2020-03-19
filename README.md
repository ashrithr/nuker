![Rust](https://github.com/ashrithr/nuker/workflows/Rust/badge.svg?branch=master)

# AWS Resource Cleaner

Cleans up AWS resources based on configurable Rules. This project is a WIP.

## Supported Services

* EC2
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up Idle Instances based on CloudWatch metrics
    - Clean up based on open Security Group rules
    - Clean up unassociated elastic IP addresses
    - Clean up unused elastic network interfaces
    - Clean up instances older than configured duration
* EBS
    - Clean up unused volumes
    - Clean up snapshots that are older than configured duration
    - Enforce use of gp2 volumes over io1
* ELB
    - Clean up based on Tags
    - Clean up Idle Load Balancers based on CloudWatch metrics
* ASG
    - Clean up based on tags
    - Clean up empty ASG (if no instances and load balancers are attached)
* RDS
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up Idle Instances/Clusters based on CloudWatch metrics
    - Clean up stopped database instances older than configured duration
* Redshift
    - Clean up based on Tags
    - Clean up based on Allowed Instance Types
    - Clean up Idle Clusters based on Cloudwatch metrics
* S3
    - Clean up based on bucket naming prefix
    - Clean up publicly exposed buckets
    - Enforce DNS compliant naming
* EMR
    - Clean up based on Tags
    - Clean up based on allowed Instance types
    - Clean up Idle Instances based on Cloudwatch metrics
    - Clean up based on open Security Group rules
    - Enforce using cluster for specified duration
* Glue
    - Clean up Glue Dev Endpoints based on Tags
    - Enforce using Glue Dev Endpoints for specified duration
* Sagemaker
    - Clean up Sagemaker Notebooks based on Tags
    - Enforce using Sagemaker Notebooks for specified duration
* ElasticSearch
    - Clean up ES Domains based on Tags
    - Clean up based on allowed Instance Types
    - Clean up Idle Domains based on Cloudwatch metrics

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

nuker can be built and run using Docker:

```
docker build -t nuker .
```

sample run commands:

1. Run nuker with specified AWS Access Key and Secret Access Key

```
docker run --rm -it \
-v "$PWD/examples/configs/sample.toml":/home/nuker/config.toml \
-e AWS_ACCESS_KEY_ID=REPLACE_WITH_ACCESS_KEY \
-e AWS_SECRET_ACCESS_KEY=REPLACE_WITH_SECRET_KEY \
ashrithr/nuker:latest \
--config /home/nuker/config.toml
```

> NOTE: Add `--no-dry-run` flag to actually clean up the resources

2. Run nuker by mounting aws credentials directory to Docker container

```
docker run --rm -it \
-v "$PWD/examples/configs/sample.toml":/home/nuker/config.toml \
-v "$HOME/.aws":/home/nuker/.aws \
ashrithr/nuker:latest \
--profile default \
--config /home/nuker/config.toml
```

> NOTE: Add `--no-dry-run` flag to actually clean up the resources