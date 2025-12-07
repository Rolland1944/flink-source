package io.pixelsdb.pixels.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.table.FileStoreTable;

import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PixelsFlinkApp {
    public static void main(String[] args) throws Exception {
        // 1. Load Properties
        Properties props = new Properties();
        try (InputStream input = PixelsFlinkApp.class.getClassLoader().getResourceAsStream("pixels-client.properties")) {
            if (input != null) {
                props.load(input);
            } else {
                System.out.println("Sorry, unable to find pixels-client.properties");
                return;
            }
        }

        // 2. Setup Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Enable checkpointing as it is required for some sinks (like Iceberg) to commit data
        env.enableCheckpointing(Long.parseLong(props.getProperty("checkpoint.interval.ms")));

        // 3. Build and Execute Job
        buildJob(env, props);
        
        env.execute("Pixels Flink App");
    }

    public static void buildJob(StreamExecutionEnvironment env, Properties props) {
        RowType rowType = parseSchema(props.getProperty("source.schema"));

        // Create Source
        PixelsRpcSource source = new PixelsRpcSource(props, rowType);
        DataStream<RowData> stream = env.addSource(source).name("PixelsRpcSource");

        // Configure Sink based on type
        String sinkType = props.getProperty("sink.type");
        if ("iceberg".equalsIgnoreCase(sinkType)) {
            configureIcebergSink(stream, props, rowType);
        } else {
            configurePaimonSink(stream, props);
        }
    }

    private static void configurePaimonSink(DataStream<RowData> stream, Properties props) {
        String warehouse = props.getProperty("paimon.catalog.warehouse");
        String tableNameStr = props.getProperty("paimon.table.name");
        
        // Parse database and table name
        String dbName = props.getProperty("paimon.database.name");
        String tblName = tableNameStr;
        if (tableNameStr.contains(".")) {
            String[] parts = tableNameStr.split("\\.");
            dbName = parts[0];
            tblName = parts[1];
        }

        if (dbName == null || dbName.trim().isEmpty()) {
            throw new RuntimeException("Paimon database name is not configured. Please set 'paimon.database.name' in properties or use 'db.table' format in 'paimon.table.name'.");
        }

        Options options = new Options();
        options.set("warehouse", warehouse);

        try {
            org.apache.paimon.catalog.Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            try {
                catalog.createDatabase(dbName, true);
            } catch (Exception e) {
                // Ignore if database creation fails, it might already exist or user doesn't have permission
            }
            org.apache.paimon.table.Table table = catalog.getTable(Identifier.create(dbName, tblName));

            new FlinkSinkBuilder((FileStoreTable) table)
                .withInput(stream)
                .build();
                
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure Paimon sink", e);
        }
    }

    private static void configureIcebergSink(DataStream<RowData> stream, Properties props, RowType rowType) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        String warehouse = props.getProperty("iceberg.catalog.warehouse");
        String tableNameStr = props.getProperty("iceberg.table.name");
        
        // Parse database and table name
        String dbName = props.getProperty("iceberg.database.name");
        String tblName = tableNameStr;
        if (tableNameStr.contains(".")) {
            String[] parts = tableNameStr.split("\\.");
            dbName = parts[0];
            tblName = parts[1];
        }

        if (dbName == null || dbName.trim().isEmpty()) {
            throw new RuntimeException("Iceberg database name is not configured. Please set 'iceberg.database.name' in properties or use 'db.table' format in 'iceberg.table.name'.");
        }

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse", warehouse);

        catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        CatalogLoader catalogLoader;
        Configuration hadoopConf = new Configuration();

        if ("glue".equalsIgnoreCase(catalogType)) {
            // AWS Glue Catalog - credentials auto-discovered from ~/.aws/credentials
            catalogProps.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            
            catalogLoader = CatalogLoader.custom("glue_catalog", catalogProps, hadoopConf, 
                "org.apache.iceberg.aws.glue.GlueCatalog");
        } else {
            throw new RuntimeException("Unsupported catalog type.");
        }

        Catalog catalog = catalogLoader.loadCatalog();
        if (catalog instanceof SupportsNamespaces) {
            try {
                SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
                Namespace ns = Namespace.of(dbName);
                if (!nsCatalog.namespaceExists(ns)) {
                    nsCatalog.createNamespace(ns);
                }
            } catch (Exception e) {
                // Ignore if namespace creation fails
            }
        }
        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);

        if (!catalog.tableExists(identifier)) {
            Type icebergType = FlinkSchemaUtil.convert(rowType);
            Schema icebergSchema = new Schema(icebergType.asStructType().fields());
            catalog.createTable(identifier, icebergSchema);
        }

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        FlinkSink.forRowData(stream)
                .tableLoader(tableLoader)
                .append();
    }

    private static RowType parseSchema(String schemaStr) {
        List<String> columns = splitSchemaString(schemaStr);
        List<String> names = new ArrayList<>();
        List<LogicalType> types = new ArrayList<>();

        for (String col : columns) {
            // Split by first colon
            int colonIndex = col.indexOf(':');
            if (colonIndex == -1) {
                throw new IllegalArgumentException("Invalid schema format. Expected 'name:type', got: " + col);
            }
            String name = col.substring(0, colonIndex).trim();
            String typeStr = col.substring(colonIndex + 1).trim().toUpperCase();

            names.add(name);
            types.add(mapType(typeStr));
        }

        return RowType.of(
                types.toArray(new LogicalType[0]),
                names.toArray(new String[0])
        );
    }

    private static List<String> splitSchemaString(String schemaStr) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        StringBuilder current = new StringBuilder();
        for (char c : schemaStr.toCharArray()) {
            if (c == ',' && depth == 0) {
                if (current.length() > 0) {
                    parts.add(current.toString().trim());
                    current.setLength(0);
                }
            } else {
                if (c == '(' || c == '<') depth++;
                if (c == ')' || c == '>') depth--;
                current.append(c);
            }
        }
        if (current.length() > 0) {
            parts.add(current.toString().trim());
        }
        return parts;
    }

    private static LogicalType mapType(String typeStr) {
        // Handle types with parameters
        if (typeStr.startsWith("DECIMAL")) {
            Matcher m = Pattern.compile("DECIMAL\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)").matcher(typeStr);
            if (m.find()) {
                int p = Integer.parseInt(m.group(1));
                int s = Integer.parseInt(m.group(2));
                return new DecimalType(p, s);
            }
            return new DecimalType(10, 0); // Default
        }
        if (typeStr.startsWith("VARCHAR")) {
             Matcher m = Pattern.compile("VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)").matcher(typeStr);
             if (m.find()) {
                 return new VarCharType(Integer.parseInt(m.group(1)));
             }
             return new VarCharType(VarCharType.MAX_LENGTH);
        }
        if (typeStr.startsWith("CHAR")) {
             Matcher m = Pattern.compile("CHAR\\s*\\(\\s*(\\d+)\\s*\\)").matcher(typeStr);
             if (m.find()) {
                 return new CharType(Integer.parseInt(m.group(1)));
             }
             return new CharType(1);
        }
        if (typeStr.startsWith("FIXED")) {
             Matcher m = Pattern.compile("FIXED\\s*\\(\\s*(\\d+)\\s*\\)").matcher(typeStr);
             if (m.find()) {
                 return new BinaryType(Integer.parseInt(m.group(1)));
             }
             throw new IllegalArgumentException("FIXED type requires length: " + typeStr);
        }

        switch (typeStr) {
            case "INT":
            case "INTEGER":
                return new IntType();
            case "STRING":
            case "VARCHAR":
                return new VarCharType(VarCharType.MAX_LENGTH);
            case "BIGINT":
            case "LONG":
                return new BigIntType();
            case "FLOAT":
                return new FloatType();
            case "DOUBLE":
                return new DoubleType();
            case "BOOLEAN":
            case "BOOL":
                return new BooleanType();
            case "BINARY":
            case "VARBINARY":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            case "DATE":
                return new DateType();
            case "TIME":
                return new TimeType();
            case "TIMESTAMP":
                return new TimestampType(6); // Default precision 6 (microseconds)
            case "TIMESTAMPTZ":
            case "TIMESTAMP_LTZ":
                return new LocalZonedTimestampType(6);
            default:
                throw new UnsupportedOperationException("Unsupported type in config: " + typeStr);
        }
    }
}
