## Amazon Web Services (AWS) Advanced .NET Wrapper

The AWS Advanced .NET Wrapper is complementary to existing .NET database drivers and aims to extend their functionality to enable applications to take full advantage of the features of clustered databases such as Amazon Aurora. The wrapper does not connect directly to any database, but enables support of AWS and Aurora functionalities on top of an underlying .NET driver of the user's choice.

## About the Wrapper

Hosting a database cluster in the cloud via Aurora is able to provide users with sets of features and configurations to obtain maximum performance and availability, such as database failover. However, at the moment, most existing drivers do not currently support those functionalities or are not able to entirely take advantage of it.

The main idea behind the AWS Advanced .NET Wrapper is to add a software layer on top of an existing .NET driver that would enable all the enhancements brought by Aurora, without requiring users to change their workflow with their databases and existing .NET drivers.

### What is Failover?

In an Amazon Aurora database cluster, **failover** is a mechanism by which Aurora automatically repairs the cluster status when a primary DB instance becomes unavailable. It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS Advanced .NET Wrapper is designed to understand the situation and coordinate with the cluster in order to provide minimal downtime and allow connections to be very quickly restored in the event of a DB instance failure.

### Benefits of the AWS Advanced .NET Wrapper

Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS Advanced .NET Wrapper allows customers to continue using their existing community drivers in addition to having the AWS Advanced .NET Wrapper fully exploit failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora DB, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS Advanced .NET Wrapper can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible.

### Enhanced Failure Monitoring

Since a database failover is usually identified by reaching a network or a connection timeout, the AWS Advanced .NET Wrapper introduces an enhanced and customizable manner to faster identify a database outage.

Enhanced Failure Monitoring (EFM) is a feature that periodically checks the connected database instance's health and availability. If a database instance is determined to be unhealthy, the connection is aborted (and potentially routed to another healthy instance in the cluster).

### Using the AWS Advanced .NET Wrapper with plain RDS databases

The AWS Advanced .NET Wrapper also works with RDS provided databases that are not Aurora.

## Security

### Important: sslInsecure Parameter

Wrapped datasources allow for an `sslInsecure` parameter that, when set to `true`, disables server certificate verification. **This configuration poses significant security risks and should never be used in production environments.**

**Security Risks:**
- Disabling certificate verification makes connections vulnerable to man-in-the-middle attacks
- Attackers could intercept and modify database communications
- Sensitive data transmitted to/from the database could be compromised

**Recommendations:**
- **Never use `sslInsecure=true` in production environments**
- Use proper SSL/TLS certificates with valid certificate chains
- For environments where standard certificate validation is problematic, implement certificate pinning as an alternative
- The wrapper will log runtime warnings when certificate validation is disabled

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

### Third Party License Acknowledgement

Although this repository is released under the Apache-2.0 License, it supports the use of the third party MySql.Data package. The MySql.Data package's licensing includes the GPL-2.0-only License with Universal-FOSS-exception-1.0.

Although this repository is released under the Apache-2.0 License, it supports the use of the third party NHibernate package. The NHibernate package's licensing includes the LGPL-2.1-only License.
