package io.pixelsdb.pixels.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.Table;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.table.FileStoreTable;

import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
        // Define Schema (Hardcoded for demo)
        RowType rowType = RowType.of(
                new LogicalType[]{new IntType(), new VarCharType()},
                new String[]{"id", "data"}
        );

        // Create Source
        PixelsRpcSource source = new PixelsRpcSource(props, rowType);
        DataStream<RowData> stream = env.addSource(source).name("PixelsRpcSource");

        // Configure Sink based on type
        String sinkType = props.getProperty("sink.type");
        if ("iceberg".equalsIgnoreCase(sinkType)) {
            configureIcebergSink(stream, props);
        } else {
            configurePaimonSink(stream, props);
        }
    }

    private static void configurePaimonSink(DataStream<RowData> stream, Properties props) {
        String warehouse = props.getProperty("paimon.catalog.warehouse");
        String tableNameStr = props.getProperty("paimon.table.name");
        
        // Parse database and table name
        String dbName = "default";
        String tblName = tableNameStr;
        if (tableNameStr.contains(".")) {
            String[] parts = tableNameStr.split("\\.");
            dbName = parts[0];
            tblName = parts[1];
        }

        Options options = new Options();
        options.set("warehouse", warehouse);

        // Configure S3 if properties exist
        if (props.containsKey("s3.access-key")) {
            options.set("s3.access-key", props.getProperty("s3.access-key"));
        }
        if (props.containsKey("s3.secret-key")) {
            options.set("s3.secret-key", props.getProperty("s3.secret-key"));
        }
        if (props.containsKey("s3.endpoint")) {
            options.set("s3.endpoint", props.getProperty("s3.endpoint"));
        }
        if (props.containsKey("s3.path.style.access")) {
            options.set("s3.path.style.access", props.getProperty("s3.path.style.access"));
        }
        
        // Enable S3 support
        options.set("hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        options.set("hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        try {
            org.apache.paimon.catalog.Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            org.apache.paimon.table.Table table = catalog.getTable(Identifier.create(dbName, tblName));

            new FlinkSinkBuilder((FileStoreTable) table)
                .withInput(stream)
                .build();
                
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure Paimon sink", e);
        }
    }

    private static void configureIcebergSink(DataStream<RowData> stream, Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "hadoop");
        String warehouse = props.getProperty("iceberg.catalog.warehouse");
        String tableNameStr = props.getProperty("iceberg.table.name");
        
        // Parse database and table name
        String dbName = "default";
        String tblName = tableNameStr;
        if (tableNameStr.contains(".")) {
            String[] parts = tableNameStr.split("\\.");
            dbName = parts[0];
            tblName = parts[1];
        }

        // Initialize Configuration
        Configuration hadoopConf = new Configuration();
        
        // Configure S3 if properties exist (for non-AWS S3 like MinIO)
        if (props.containsKey("s3.access-key")) {
            hadoopConf.set("fs.s3a.access.key", props.getProperty("s3.access-key"));
        }
        if (props.containsKey("s3.secret-key")) {
            hadoopConf.set("fs.s3a.secret.key", props.getProperty("s3.secret-key"));
        }
        if (props.containsKey("s3.endpoint")) {
            hadoopConf.set("fs.s3a.endpoint", props.getProperty("s3.endpoint"));
        }
        if (props.containsKey("s3.path.style.access")) {
            hadoopConf.set("fs.s3a.path.style.access", props.getProperty("s3.path.style.access"));
        }

        // Enable S3 support
        hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        CatalogLoader catalogLoader;
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse", warehouse);

        if ("glue".equalsIgnoreCase(catalogType)) {
            // AWS Glue Catalog - credentials auto-discovered from ~/.aws/credentials
            catalogProps.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            
            // Optional: Specify AWS region if not in ~/.aws/config
            if (props.containsKey("iceberg.catalog.glue.region")) {
                catalogProps.put("glue.region", props.getProperty("iceberg.catalog.glue.region"));
            }
            
            catalogLoader = CatalogLoader.custom("glue_catalog", catalogProps, hadoopConf, 
                "org.apache.iceberg.aws.glue.GlueCatalog");
        } else if ("rest".equalsIgnoreCase(catalogType)) {
            String uri = props.getProperty("iceberg.catalog.uri");
            String credential = props.getProperty("iceberg.catalog.credential");
            
            catalogProps.put("uri", uri);
            catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            if (credential != null) {
                catalogProps.put("credential", credential);
            }
            
            // Pass S3 properties to Catalog properties (needed for S3FileIO)
            if (props.containsKey("s3.endpoint")) {
                catalogProps.put("s3.endpoint", props.getProperty("s3.endpoint"));
            }
            if (props.containsKey("s3.access-key")) {
                catalogProps.put("s3.access-key-id", props.getProperty("s3.access-key"));
            }
            if (props.containsKey("s3.secret-key")) {
                catalogProps.put("s3.secret-access-key", props.getProperty("s3.secret-key"));
            }
            if (props.containsKey("s3.path.style.access")) {
                catalogProps.put("s3.path-style-access", props.getProperty("s3.path.style.access"));
            }

            catalogLoader = CatalogLoader.custom("rest_catalog", catalogProps, hadoopConf, "org.apache.iceberg.rest.RESTCatalog");
        } else {
            // Hadoop Catalog (can still work with S3)
            catalogLoader = CatalogLoader.hadoop("hadoop_catalog", hadoopConf, catalogProps);
        }

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);
        Table table = catalog.loadTable(identifier);
        
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        FlinkSink.forRowData(stream)
                .tableLoader(tableLoader)
                .equalityFieldColumns(new ArrayList<>(table.schema().identifierFieldNames()))
                .append();
    }
}
