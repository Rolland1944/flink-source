# Pixels Flink Source

## Introduction
`pixels-flink-source` is a custom Apache Flink Source Connector that retrieves data in real-time from a Pixels-sink server via gRPC. It supports writing data to data lakes such as Apache Iceberg and Apache Paimon.

## Features
*   **RPC Source**: Fetches data using gRPC polling mechanism.
*   **Multi-Sink Support**: Integrated `PixelsFlinkApp` application supports sinking data to both **Apache Paimon** and **Apache Iceberg**.
*   **Configuration**: Driven by `pixels-client.properties` for easy configuration of connections and catalogs.

## Build

Ensure Maven 3.6+ and Java 11 are installed.

```bash
mvn clean package
```

The build artifact will be located at `target/pixels-flink-source-1.0-SNAPSHOT.jar`.

## Run Tests

This project includes integration tests to verify the Source functionality and Sink integrations.

```bash
mvn clean test
```

## Run Application

The project includes a main application class `io.pixelsdb.pixels.flink.PixelsFlinkApp`, which reads the configuration file and starts a Flink job to synchronize Pixels data to the configured data lake.

### 1. Configuration
Modify `src/main/resources/pixels-client.properties` (or replace the file in classpath at runtime):

```properties
# Pixels Server Configuration
pixels.server.host=localhost
pixels.server.port=50051
table.name=orders

# Sink Type (paimon or iceberg)
sink.type=paimon

# Paimon Configuration
paimon.catalog.type=paimon
paimon.catalog.warehouse=file:///tmp/paimon
paimon.table.name=paimon_orders

# Iceberg Configuration
iceberg.catalog.type=hadoop
iceberg.catalog.warehouse=file:///tmp/iceberg
iceberg.table.name=iceberg_orders
```

### 2. Submit Job
Submit the job using Flink:

```bash
flink run -c io.pixelsdb.pixels.flink.PixelsFlinkApp target/pixels-flink-source-1.0-SNAPSHOT.jar
```

## Code Example (Custom Development)

If you need to develop a custom Flink job:

```java
import io.pixelsdb.pixels.flink.PixelsRpcSource;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.IntType;
import java.util.Properties;

// ...

Properties props = new Properties();
props.setProperty("pixels.server.host", "localhost");
props.setProperty("pixels.server.port", "50051");
props.setProperty("table.name", "orders");

// Define Schema (Matching Pixels table structure)
RowType rowType = RowType.of(
    new LogicalType[] { new IntType(), new VarCharType() },
    new String[] { "id", "data" }
);

// Add Source
DataStream<RowData> stream = env.addSource(new PixelsRpcSource(props, rowType));
// ...
