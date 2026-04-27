# Blue/Green Deployment Plugin

## What is Blue/Green Deployment?

The [Blue/Green Deployment](https://docs.aws.amazon.com/whitepapers/latest/blue-green-deployments/introduction.html) technique enables organizations to release applications by seamlessly shifting traffic between two identical environments running different versions of the application. This strategy effectively mitigates common risks associated with software deployment, such as downtime and limited rollback capability.

The AWS Advanced .Net Data Provider Wrapper leverages the Blue/Green Deployment approach by intelligently managing traffic distribution between blue and green nodes, minimizing the impact of stale DNS data and connectivity disruptions on user applications.

## Prerequisites
- AWS cluster and instance endpoints must be directly accessible from the client side
- :warning: Extra permissions are required for non-admin users so that the blue/green metadata table/function can be properly queried. If the permissions are not granted, the metadata table/function will not be visible and blue/green plugin functionality will not work properly. Please see the [Connecting with non-admin users](#connecting-with-non-admin-users) section below.

> [!WARNING]\
> Currently Supported Database Deployments:
> - Aurora MySQL and PostgreSQL clusters
> - RDS MySQL and PostgreSQL instances
>
> Unsupported Database Deployments and Configurations:
> - RDS MySQL and PostgreSQL Multi-AZ clusters
> - Aurora Global Database for MySQL and PostgreSQL
>
> Additional Requirements:
> - Connecting to database nodes using CNAME aliases is not supported
>
> **Blue/Green Support Behaviour and Version Compatibility:**
>
> The AWS Advanced .Net Data Provider Wrapper now includes enhanced full support for Blue/Green Deployments. This support requires a minimum database version that includes a specific metadata table. The metadata will be accessible provided the green deployment satisfies the minimum version compatibility requirements. This constraint **does not** apply to RDS MySQL.
>
> For RDS Postgres, you will also need to manually install the `rds_tools` extension using the following DDL so that the metadata required by the wrapper is available:
>
> ```sql
> CREATE EXTENSION rds_tools;
> ```
>
> If your database version does **not** support this table, the driver will automatically detect its absence and fallback to its previous behaviour. In this fallback mode, Blue/Green handling is subject to the same limitations listed above.
>
> **No action is required** if your database does not include the new metadata table -- the driver will continue to operate as before. If you have questions or encounter issues, please open an issue in this repository.
>
> Supported RDS PostgreSQL Versions: `rds_tools v1.7 (17.1, 16.5, 15.9, 14.14, 13.17, 12.21)` and above.<br>
> Supported Aurora PostgreSQL Versions: Engine Release `17.5, 16.9, 15.13, 14.18, 13.21` and above.<br>
> Supported Aurora MySQL Versions: Engine Release `3.07` and above.


## What is Blue/Green Deployment Plugin?

During a [Blue/Green switchover](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html), several significant changes occur to your database configuration:
- Connections to blue nodes terminate at a specific point during the transition
- Node connectivity may be temporarily impacted due to reconfigurations and potential node restarts
- Cluster and instance endpoints are redirected to different database nodes
- Internal database node names undergo changes
- Internal security certificates are regenerated to accommodate the new node names


All factors mentioned above may cause application disruption. The AWS Advanced .Net Data Provider Wrapper aims to minimize the application disruption during Blue/Green switchover by performing the following actions:
- Actively monitors Blue/Green switchover status and implements appropriate measures to suspend, pass-through, or re-route database traffic
- Prior to Blue/Green switchover initiation, compiles a comprehensive inventory of cluster and instance endpoints for both blue and green nodes along with their corresponding IP addresses
- During the active switchover phase, temporarily suspends execution of database calls to blue nodes, which helps unload database nodes and reduces transaction lag for green nodes, thereby enhancing overall switchover performance
- Substitutes provided hostnames with corresponding IP addresses when establishing new blue connections, effectively eliminating stale DNS data and ensuring connections to current blue nodes
- During the brief post-switchover period, continuously monitors DNS entries, confirms that blue endpoints have been reconfigured, and discontinues hostname-to-IP address substitution as it becomes unnecessary
- Automatically rejects new connection requests to green nodes when the switchover is completed but DNS entries for green nodes remain temporarily available
- Intelligently detects switchover failures and rollbacks to the original state, implementing appropriate connection handling measures to maintain application stability

## How do I use Blue/Green Deployment Plugin with the AWS Advanced .Net Data Provider Wrapper?

To enable the Blue/Green Deployment functionality, add the plugin code `bg` to the [`Plugins`](../UsingTheDotNetDataProviderDriver.md#connection-plugin-manager-parameters) parameter value.
The Blue/Green Deployment Plugin supports the following configuration parameters:

| Parameter               |  Value  |                           Required                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                             | Example Value            | Default Value |
|-------------------------|:-------:|:------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|---------------|
| `BgdId`                 | String  | If using multiple Blue/Green Deployments, yes; otherwise, no | This parameter is optional and defaults to `1`. When supporting multiple Blue/Green Deployments (BGDs), this parameter becomes mandatory. Each connection string must include the `BgdId` parameter with a value that can be any number or string. However, all connection strings associated with the same Blue/Green Deployment must use identical `BgdId` values, while connection strings belonging to different BGDs must specify distinct values. | `1234`, `abc-1`, `abc-2` | `1`           |
| `BgConnectTimeoutMs`    | Integer |                              No                              | Maximum waiting time (in milliseconds) for establishing new connections during a Blue/Green switchover when blue and green traffic is temporarily suspended.                                                                                                                                                                                                                                                                                            | `30000`                  | `30000`       |
| `BgBaselineMs`          | Integer |                              No                              | The baseline interval (ms) for checking Blue/Green Deployment status. It's highly recommended to keep this parameter below 900000ms (15 minutes).                                                                                                                                                                                                                                                                                                       | `60000`                  | `60000`       |
| `BgIncreasedMs`         | Integer |                              No                              | The increased interval (ms) for checking Blue/Green Deployment status. Configure this parameter within the range of 500-2000 milliseconds.                                                                                                                                                                                                                                                                                                              | `1000`                   | `1000`        |
| `BgHighMs`              | Integer |                              No                              | The high-frequency interval (ms) for checking Blue/Green Deployment status. Configure this parameter within the range of 50-500 milliseconds.                                                                                                                                                                                                                                                                                                           | `100`                    | `100`         |
| `BgSwitchoverTimeoutMs` | Integer |                              No                              | Maximum duration (in milliseconds) allowed for switchover completion. If the switchover process stalls or exceeds this timeframe, the driver will automatically assume completion and resume normal operations.                                                                                                                                                                                                                                         | `180000`                 | `180000`      |

The plugin establishes dedicated monitoring connections to track Blue/Green Deployment status. To apply specific configurations to these monitoring connections, add the `blue-green-monitoring-` prefix to any configuration parameter, as shown in the following example:

```dotnet
AwsWrapperConnection<NpgsqlConnection> connection = new(
        "Host=database.cluster-xyz.us-east-1.rds.amazonaws.com;
        Database=mysql;
        Username=admin;
        Password=pwd;
        Plugins=bg;
        // Configure the command timeout values for all non-monitoring connections.
        topology-monitoring-commandTimeout=30000;
        // Configure a different command timeout values for the Blue/Green monitoring connections.
        blue-green-monitoring-commandTimeout=10000;"
);
```

## Connecting with non-admin users

> [!WARNING]\
> The following permissions are **required** for every non-admin user account connecting to the DB instance/cluster.
> If the permissions are not granted, the metadata table/function will not be visible and blue/green plugin functionality will not work properly.

| Environment       | Required permission statements                                                                                        |
|-------------------|-----------------------------------------------------------------------------------------------------------------------|
| Aurora Postgresql | None                                                                                                                  |
| RDS Postgresql    | `GRANT USAGE ON SCHEMA rds_tools TO your_user;`<br>`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA rds_tools TO your_user;` |
| Aurora MySQL      | `GRANT SELECT ON mysql.rds_topology TO 'your_user'@'%';`<br>`FLUSH PRIVILEGES;`                                       |
| RDS MySQL         | `GRANT SELECT ON mysql.rds_topology TO 'your_user'@'%';`<br>`FLUSH PRIVILEGES;`                                       |

In MySQL, you can leverage MySQL Role to grant the required permissions to multiple users at once to reduce operational overhead before switchover.
See instructions in [Using Roles in MySQL 8.0 to Grant Privileges to mysql.rds_topology](./GrantingPermissionsToNonAdminUserInMySQL.md).

## Plan your Blue/Green switchover in advance

To optimize Blue/Green switchover support with the AWS Advanced .Net Data Provider Wrapper, advance planning is essential. Please follow these recommended steps:

1. Create a Blue/Green Deployment for your database.
2. If you're planning to connect with a non-admin user, add the necessary permissions to allow the plugin to access Blue/Green metadata. Make sure that permissions are granted on **both** blue and green clusters.
3. Configure your application by incorporating the `bg` plugin along with any additional parameters of your choice, then deploy your application to the corresponding environment.
4. The order of steps 1 and 3 is flexible and can be performed in either sequence.
5. Allow sufficient time for the deployed application with the active Blue/Green plugin to collect deployment status information. This process typically requires several minutes.
6. Initiate the Blue/Green Deployment switchover through the AWS Console, CLI, or RDS API.
7. Monitor the process until the switchover completes successfully or rolls back. This may take several minutes.
8. Review the switchover summary in the application logs. This requires setting the log level to `Trace` either through the `LOG_LEVEL` environmental variable or custom implementation of `ILoggerFactory`.
9. Update your application by deactivating the `bg` plugin through its removal from your application configuration. Redeploy your application afterward. Note that an active Blue/Green plugin produces no adverse effects once the switchover has been completed.
10. Delete the Blue/Green Deployment through the appropriate AWS interface.
11. The sequence of steps 9 and 10 is flexible and can be executed in either order based on your preference.

Here's an example of a switchover summary. Time zero corresponds to the beginning of the active switchover phase. Time offsets indicate the start time of each specific switchover phase.
```
[Information] AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection.BlueGreenStatusProvider: [bgdId: '1']
---------------------------------------------------------------------------------------
timestamp                         time offset (ms)                                event
---------------------------------------------------------------------------------------
       3/18/2026 12:53:59 AM             -21703 ms                              CREATED
       3/18/2026 12:54:18 AM              -2623 ms                          PREPARATION
       3/18/2026 12:54:21 AM                  0 ms               Monitors reset - start
       3/18/2026 12:54:21 AM                  0 ms                          IN_PROGRESS
       3/18/2026 12:54:24 AM               3391 ms                                 POST
       3/18/2026 12:54:31 AM               9632 ms               Green topology changed
       3/18/2026 12:54:31 AM               9632 ms      Monitors reset - green topology
       3/18/2026 12:54:38 AM              17345 ms                     Blue DNS updated
       3/18/2026 12:54:51 AM              29660 ms                    Green DNS removed
       3/18/2026 12:54:51 AM              29661 ms                            COMPLETED
---------------------------------------------------------------------------------------
```
