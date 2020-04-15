![Rust](https://github.com/ashrithr/nuker/workflows/Rust/badge.svg?branch=master)

# AWS Resource Cleaner

Cleans up AWS resources based on configurable Rules. This project is a WIP.

## Supported Resources

The following table illustrates supported resource types and rules that are
supported for marking the resources to cleanup.

* required-tags - cleanup resources based on the specified tags
* approved instance types - ensures that the resources provisioned are not using
resource types that are not approved.
* idle-rules - identifies if a resource is idle or not based on the configured
cloudwatch metrics.
* Manage Stopped - if enabled, cleans up resource if the resource is stopped for
a more than specified duration.
* Max Runtime - ensures that the resource provisioned is only running for approved
amount of time.

In addition to the specified rules above, each resource can have their own
additional rules, which are defined below.

| resource type | required-tags | approved-types | idle-rules | max-run-time | manage-stopped | additional-rules |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| ec2-instance | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |:heavy_check_mark: | - |
| ec2-address | :heavy_check_mark: | :x: | :x: | :x: | :x: | [rules](#ec2-address-rules) |
| ec2-eni | :heavy_check_mark: | :x: | :x: | :x: | :x: | [rules](#ec2-eni-rules) |
| ec2-sg | :heavy_check_mark: | :x: | :x: | :x: | :x: | [rules](#ec2-sg-rules) |
| ec2-vpc | :heavy_check_mark: | :x: | :x: | :x: | :x: | - |
| asg | :heavy_check_mark: | :x: | :x: | :x: | :x: | [rules](#asg-rules) |
| ebs-volume | :heavy_check_mark: | :heavy_check_mark: | :x: | :x: | :x: | [rules](#ebs-volume-rules) |
| ebs-snapshot | :heavy_check_mark: | :x: | :x: | :heavy_check_mark: | :x: | - |
| ecs-cluster | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :x: | :x: | - |
| eks-cluster | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :x: | :x: | - |
| elb-alb | :heavy_check_mark: | :x: | :heavy_check_mark: | :x: | :x: | - |
| elb-nlb | :heavy_check_mark: | :x: | :heavy_check_mark: | :x: | :x: | - |
| emr-cluster | :heavy_exclamation_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :x: | - |
| es-domain | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :x: | - |
| glue-endpoint | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :x: | - |
| rds-cluster | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | - |
| rds-instance | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | - |
| s3-bucket| :heavy_check_mark: | :x: | :x: | :x: | :x: | [rules](#s3-bucket-rules) |
| sagemaker-notebook | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :x: | - |

### Additional Rules

#### ec2-address rules

- Unassociated - Removes an EIP that is unassociated

#### ec2-eni rules

- Unassociated - Removes an ENI that is unassociated

#### ec2-sg rules

- Unsecured - Removes an Security Group and the resources that are using it, if
open to the world rules (0.0.0.0/0 or ::/0) are associated with it.

#### asg rules

- Unassociated - Removes an Auto Scaling Group that has no instances associated,
or no ELB's associated with it.

#### ebs-volume rules

- Unassociated - Removes a Volume that is unassociated

#### s3-bucket rules

- Bucket Naming Prefix - Ensures bucket naming convention is followed
- Publicly Accessability - Ensures bucket is not publicly accessible
- DNS Compliant Naming - Ensures bucket naming convention to be DNS compliant

## Configuration

nuker requires a configuration file to operate with, sample configuration is located for reference here: `examples/config/sample.toml`.

Make a copy of the sample configuration and make changes as needed based on the comments provided in the sample configuration file.

```
cp examples/config/sample.toml config.toml
```

### Whitelisting Resources

Each resource type supports the ability to whitelist resources from the config
file. Refer to sample configuration file for examples.

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

## Targeting/Excluding specific resources

To Target/Exclude specific resource types use `--target` or `--exclude` flags. For example:

```
nuker --config examples/configs/sample.toml \
--profile default \
--region us-east-1 \
--exclude s3 \
--exclude es
```

> To view list of supported resource types, use the `nuker resource-types`.

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