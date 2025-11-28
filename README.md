# Pixels Flink Source

## Introduction
`pixels-flink-source` is a custom Apache Flink Source Connector designed to ingest data in real-time from a Pixels-sink server using gRPC polling. It enables seamless data synchronization from Pixels to modern data lakes, specifically supporting **Apache Iceberg** (with AWS Glue Catalog) and **Apache Paimon**, backed by S3 storage.

## Features
*   **RPC Source**: High-performance data fetching via gRPC polling mechanism.
*   **Schema Mapping**: Configurable source schema definition to map Pixels data types to Flink RowData.
*   **Multi-Sink Support**: Integrated support for writing to:
    *   **Apache Iceberg**: Supports AWS Glue Catalog and S3 storage.
    *   **Apache Paimon**: Supports S3 filesystem catalog.
*   **Cloud Native**: Built with AWS SDK v2, ready for cloud deployments with S3 and Glue integration.
*   **Configurable**: Fully driven by `pixels-client.properties` for easy management of connections, schemas, and catalogs.

## Prerequisites
*   Java 11+
*   Maven 3.6+
*   AWS Credentials (for S3 and Glue access)

## Build

Clone the repository and build the project using Maven:

```bash
mvn clean package
```

The build artifact will be located at `target/pixels-flink-source-1.0-SNAPSHOT.jar`.

## Configuration

The application is configured via `src/main/resources/pixels-client.properties`. You can modify this file before building, or provide a modified version at runtime on the classpath.

### Core Configuration
```properties
# Pixels Server Connection
pixels.server.host=localhost
pixels.server.port=50051
schema.name=public
table.name=orders

# Source Schema Definition
# Format: col1:TYPE,col2:TYPE
# Supported Types: INT, BIGINT, STRING, FLOAT, DOUBLE, BOOLEAN
source.schema=id:INT,data:STRING,price:DOUBLE

# Flink Checkpoint Interval (Required for streaming sinks)
checkpoint.interval.ms=10000
```

### Sink Configuration

Select the target sink type:
```properties
# Options: iceberg, paimon
sink.type=iceberg
```

#### Apache Iceberg (AWS Glue Catalog)
```properties
# Catalog Type: glue (Recommended for AWS), hadoop, rest
iceberg.catalog.type=glue

# S3 Warehouse Path
iceberg.catalog.warehouse=s3://your-bucket/iceberg/
iceberg.table.name=iceberg_orders

# Optional: AWS Region (if not set in ~/.aws/config)
# iceberg.catalog.glue.region=us-east-1
```

#### Apache Paimon
```properties
# Catalog Type
paimon.catalog.type=paimon

# S3 Warehouse Path
paimon.catalog.warehouse=s3://your-bucket/paimon/
paimon.table.name=paimon_orders
```

## AWS Setup

To use S3 and AWS Glue, you must configure AWS credentials. The application uses the default AWS credential provider chain, looking for credentials in the following order:

1.  **Environment Variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
2.  **System Properties**: `aws.accessKeyId`, `aws.secretKey`
3.  **Credentials File**: `~/.aws/credentials`
4.  **Config File**: `~/.aws/config`
5.  **Instance Profile**: EC2/EKS IAM Role

### IAM Permissions
Ensure your IAM identity has permissions for:
*   **S3**: `GetObject`, `PutObject`, `ListBucket`, `DeleteObject` on your warehouse bucket.
*   **Glue**: `CreateDatabase`, `CreateTable`, `GetTable`, `UpdateTable`, etc., if using Iceberg with Glue Catalog.
