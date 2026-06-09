# Understanding the ClusterId Parameter

## Overview

The `ClusterId` parameter is a key configuration setting when using the AWS Advanced .NET Data Provider Wrapper to **connect to multiple database clusters within a single application**. This parameter is a unique identifier that lets the wrapper maintain separate caches and state for each distinct database cluster your application connects to.

## What is a Cluster?

Understanding what makes up a cluster is important for setting the `ClusterId` parameter correctly. In the context of the AWS Advanced .NET Data Provider Wrapper, a **cluster** is a logical grouping of database instances that should share the same topology cache and monitoring services.

A cluster represents one writer instance (primary) and zero or more reader instances (replicas). Together they make up the shared topology the wrapper needs to track, and they are the group of instances the wrapper can reconnect to when a failover is detected.

### Examples of Clusters

- Aurora DB Cluster (one writer + multiple readers)
- RDS Multi-AZ DB Cluster (one writer + two readers)
- Aurora Global Database (when supplying a global database endpoint, the wrapper treats the instances as a single cluster)

> **Rule of thumb:** :thumbsup: If the wrapper should track separate topology information and perform independent failover operations, use different `ClusterId` values. If instances share the same topology and failover domain, use the same `ClusterId`.

## Why ClusterId is Important

The AWS Advanced .NET Data Provider Wrapper uses the `ClusterId` as a **key for internal caching mechanisms** to optimize performance and maintain cluster-specific state. Without proper `ClusterId` configuration, your application may experience:

- Cache collisions between different clusters
- Incorrect topology information
- Degraded performance due to cache invalidation

## Why Not Use AWS DB Cluster Identifiers?

Host information can take many forms:

- **IP Address Connections:** `Host=10.0.1.50;Port=3306;Database=mydb` ← No cluster info!
- **Custom Domain Names:** `Host=db.mycompany.com;Port=3306;Database=mydb` ← Custom domain
- **Custom Endpoints:** `Host=my-custom-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com;Port=3306;Database=mydb` ← Custom endpoint
- **Proxy Connections:** `Host=my-proxy.proxy-abc.us-east-1.rds.amazonaws.com;Port=3306;Database=mydb` ← Proxy, not the actual cluster

In fact, all of these could reference the exact same cluster. Because the wrapper cannot reliably parse cluster information from all connection types, **it is up to you to explicitly provide the `ClusterId`**.

## How ClusterId is Used Internally

The wrapper uses `ClusterId` as a cache key for topology information and monitoring services. This lets multiple connections to the same cluster share cached data and avoid redundant database metadata queries.

### Example: Single Cluster with Multiple Connections

The following diagram shows how connections with the same `ClusterId` share cached resources:

![Single Cluster Example](../images/cluster_id_one_cluster_example.jpg)

**Key Points:**
- Three connections use different connection strings (custom endpoint, IP address, cluster endpoint) but all specify **`ClusterId: "foo"`**
- All three connections share the same Topology Cache and Monitor Threads in the wrapper
- The Topology Cache stores a key-value mapping where `"foo"` maps to `["instance-1", "instance-2", "instance-3"]`
- Despite the different connection strings, all connections monitor and query the same physical database cluster

**The Impact:** Shared resources eliminate redundant topology queries and reduce monitoring overhead.

### Example: Multiple Clusters with Separate Cache Isolation

The following diagram shows how different `ClusterId` values maintain separate caches for different clusters.

![Two Cluster Example](../images/cluster_id_two_cluster_example.jpg)

**Key Points:**
- Connection 1 and Connection 3 use **`ClusterId: "foo"`** and share the same cache entries
- Connection 2 uses **`ClusterId: "bar"`** and has completely separate cache entries
- Each `ClusterId` acts as a key in the cache: `"foo"` → `[instance-1, instance-2, instance-3]` and `"bar"` → `[instance-4, instance-5]`
- The Monitor Cache maintains separate monitor threads for each cluster
- Monitors poll their respective database clusters and update the corresponding topology cache entries

**The Impact:** This isolation prevents cache collisions and ensures correct failover behavior for each cluster.

## When to Specify ClusterId

### Required: Multiple Clusters in One Application

You **must** specify a unique `ClusterId` for every DB cluster when your application connects to multiple database clusters:

```dotnet
// Sample data migration app
var sourceConnectionString =
    "Host=source-db.us-east-1.rds.amazonaws.com;Port=3306;Database=mydb;" +
    "User Id=admin;Password=***;ClusterId=source-cluster;Plugins=failover,efm;";
using var sourceConnection = new AwsWrapperConnection<MySqlConnection>(sourceConnectionString);

var destConnectionString =
    "Host=dest-db.us-west-2.rds.amazonaws.com;Port=3306;Database=mydb;" +
    "User Id=admin;Password=***;ClusterId=destination-cluster;Plugins=failover,efm;"; // Different ClusterId!
using var destConnection = new AwsWrapperConnection<MySqlConnection>(destConnectionString);

// Read from source, write to destination
// ... migration logic ...

// If you connect to source-db with a different connection string later, use the same ClusterId.
var sourceIpConnectionString =
    "Host=10.0.1.50;Port=3306;Database=mydb;" +
    "User Id=admin;Password=***;ClusterId=source-cluster;Plugins=failover,efm;"; // Same ClusterId as the source connection
using var sourceIpConnection = new AwsWrapperConnection<MySqlConnection>(sourceIpConnectionString);
```

### Optional: Single Cluster Applications

If your application only connects to one cluster, you can omit `ClusterId` (it defaults to `"1"`):

```dotnet
// Single cluster - ClusterId defaults to "1"
var connectionString =
    "Host=my-cluster.cluster-abc.us-east-1.rds.amazonaws.com;Port=3306;Database=mydb;" +
    "User Id=admin;Password=***;Plugins=failover,efm;";
using var connection = new AwsWrapperConnection<MySqlConnection>(connectionString);
```

This also applies when you have multiple connections that use different host information for the same cluster:

```dotnet
// Cluster endpoint - ClusterId defaults to "1"
var urlConnectionString =
    "Host=my-cluster.cluster-abc.us-east-1.rds.amazonaws.com;Port=3306;Database=mydb;User Id=admin;Password=***;";
using var urlConnection = new AwsWrapperConnection<MySqlConnection>(urlConnectionString);

// "10.0.1.50" is the IP address of the same cluster, so ClusterId still defaults to "1"
var ipConnectionString =
    "Host=10.0.1.50;Port=3306;Database=mydb;User Id=admin;Password=***;";
using var ipConnection = new AwsWrapperConnection<MySqlConnection>(ipConnectionString);
```

## Critical Warnings

### 🚨 NEVER Share ClusterId Between Different Clusters

Using the same `ClusterId` for different database clusters will cause serious issues:

```dotnet
// ❌ WRONG - Same ClusterId for different clusters
var sourceConnectionString =
    "Host=source-db.us-east-1.rds.amazonaws.com;Port=3306;Database=db;" +
    "User Id=admin;Password=***;ClusterId=shared-id;"; // ← BAD!
using var sourceConnection = new AwsWrapperConnection<MySqlConnection>(sourceConnectionString);

var destConnectionString =
    "Host=dest-db.us-west-2.rds.amazonaws.com;Port=3306;Database=db;" +
    "User Id=admin;Password=***;ClusterId=shared-id;"; // ← BAD! Same ClusterId for a different cluster
using var destConnection = new AwsWrapperConnection<MySqlConnection>(destConnectionString);
```

**Problems this causes:**
- Topology cache collision (dest-db's topology could overwrite source-db's)
- Incorrect failover behavior (the wrapper may try to fail over to the wrong cluster)
- Monitor conflicts (a single monitor instance for both clusters leads to undefined results)

**Correct approach:**
```dotnet
// ✅ CORRECT - Unique ClusterId for each cluster
// ...ClusterId=source-cluster;
// ...ClusterId=destination-cluster;
```

### ⚠️ Always Use the Same ClusterId for the Same Cluster

Using different `ClusterId` values for the same cluster reduces efficiency:

```dotnet
// ⚠️ SUBOPTIMAL - Different ClusterIds for the same cluster
var connectionString1 =
    "Host=my-cluster.cluster-abc.us-east-1.rds.amazonaws.com;Port=3306;Database=db;" +
    "User Id=admin;Password=***;ClusterId=my-cluster-1;";
using var connection1 = new AwsWrapperConnection<MySqlConnection>(connectionString1);

var connectionString2 =
    "Host=my-cluster.cluster-abc.us-east-1.rds.amazonaws.com;Port=3306;Database=db;" +
    "User Id=admin;Password=***;ClusterId=my-cluster-2;"; // Different ClusterId for the same cluster
using var connection2 = new AwsWrapperConnection<MySqlConnection>(connectionString2);
```

**Problems this causes:**
- Duplication of caches
- Multiple monitoring threads for the same cluster

**Best practice:** Use the same `ClusterId` value for every connection to the same cluster so they share cached resources.

## Summary

The `ClusterId` parameter is essential for applications connecting to multiple database clusters. It serves as a cache key for topology information and monitoring services. Always use unique `ClusterId` values for different clusters, and consistent values for the same cluster, to maximize performance and avoid conflicts.
