Cloudwatch Commands:

List the available metrics that can be queried for EC2

```
aws cloudwatch list-metrics --namespace "AWS/EC2" --region us-east-1
aws cloudwatch list-metrics --namespace "AWS/EC2" --metric-name "CPUUtilization" --region us-east-1
```

Get metric data for specific ec2 instance:

```
 aws cloudwatch get-metric-statistics --namespace "AWS/EC2" --metric-name "CPUUtilization" --start-time 2019-10-20T00:00:00Z --end-time 2019-10-25T00:00:00Z --period 3600 --statistics Maximum --dimensions "Name=InstanceId,Value=i-0e8608607733ea6c1" --region us-east-1
 ```

 ```
 aws cloudwatch get-metric-statistics --namespace "AWS/RDS" --metric-name "CPUUtilization" --dimensions "Name=DBClusterIdentifier,Value=database-tbd" --start-time 2020-01-01T00:00:00Z --end-time 2020-02-01T00:00:00Z --period 3600 --statistics Maximum
 ```