# Support for Amazon RDS Multi-AZ DB Cluster

In addition to Aurora database clusters, the AWS Advanced .NET Data Provider Wrapper supports the Amazon RDS Multi-AZ DB Cluster Deployment. By leveraging the topology information within the RDS Multi-AZ DB Cluster, the driver is capable of switching over the connection to a new writer host in approximately 1 second or less, given there is no replica lag during minor version upgrades or OS maintenance upgrades.

## General Usage

The process of using the AWS Advanced .NET Data Provider Wrapper with RDS Multi-AZ DB Cluster is the same as using it with an Aurora cluster. All properties, configurations, functions, etc., remain consistent. Instead of connecting to a generic database endpoint, simply replace the endpoint with the Cluster Writer Endpoint provided by the RDS Multi-AZ DB Cluster.

### MySQL

Preparing a connection with MySQL in a Multi-AZ Cluster remains the same as before:

```dotnet
AwsWrapperConnection<NpgsqlConnection> connection = new(
        "Host=database.cluster-xyz.us-east-1.rds.amazonaws.com;
        Database=mysql;
        Username=admin;
        Password=pwd;
        Plugins=failover"
);
```

### PostgreSQL

The topology information is populated in Amazon RDS for PostgreSQL versions 13.12, 14.9, 15.4, or higher, starting from revision R3. Ensure you have a supported PostgreSQL version deployed.

Per AWS documentation, the `rds_tools` extension must be manually installed using the following DDL before the topology information becomes available on target cluster:

```sql
CREATE EXTENSION rds_tools;
```

Then, prepare the connection with:

```dotnet
AwsWrapperConnection<NpgsqlConnection> connection = new(
        "Host=database.cluster-xyz.us-east-1.rds.amazonaws.com;
        Database=mysql;
        User ID=admin;
        Password=pwd;
        Plugins=failover"
);
```

## Optimizing Switchover Time

Amazon RDS Multi-AZ with two readable standbys now supports minor version upgrades with 1 second of downtime.

See feature announcement [here](https://aws.amazon.com/about-aws/whats-new/2023/11/amazon-rds-multi-az-two-stanbys-upgrades-downtime/).

During minor version upgrades of RDS Multi-AZ DB clusters, the `failover` plugin switches the connection from the current writer to a newly upgraded reader. 

For more details on the `failover` plugin configuration, refer to the [Failover Configuration Guide](FailoverConfigurationGuide.md).


## Limitations

The following plugins have been tested and confirmed to work with Amazon RDS Multi-AZ DB Clusters:

* [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md)
* [Host Monitoring Connection Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)

The compatibility of other plugins has not been tested at this time. They may function as expected or potentially result in unhandled behavior.
Use at your own discretion.
