package io.pixelsdb.pixels.flink;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.ColumnValue;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.OperationType;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollRequest;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollResponse;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.RowRecord;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.RowValue;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.types.Types;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PixelsE2ETest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static Server server;
    private static int port;
    private static final BlockingQueue<PollResponse> responseQueue = new LinkedBlockingQueue<>();

    @BeforeClass
    public static void startServer() throws IOException {
        server = ServerBuilder.forPort(0)
                .addService(new MockPixelsPollingService())
                .build()
                .start();
        port = server.getPort();
        System.out.println("Mock Server started on port " + port);
    }

    @AfterClass
    public static void stopServer() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPaimonEndToEnd() throws Exception {
        runTest("paimon");
    }

    @Test
    public void testIcebergEndToEnd() throws Exception {
        runTest("iceberg");
    }

    private void runTest(String sinkType) throws Exception {
        // 1. Prepare Data
        prepareData();
        
        // Load Properties
        Properties props = new Properties();
        try (InputStream input = PixelsE2ETest.class.getClassLoader().getResourceAsStream("pixels-client.properties")) {
            if (input != null) {
                props.load(input);
            } else {
                throw new RuntimeException("pixels-client.properties not found");
            }
        }

        // Override Properties for Test
        props.setProperty("pixels.server.host", "localhost");
        props.setProperty("pixels.server.port", String.valueOf(port));
        props.setProperty("sink.type", sinkType);
        // Enable fast checkpointing for tests
        props.setProperty("checkpoint.interval.ms", "500");
        
        String tableName;
        String warehouse;

        if ("paimon".equals(sinkType)) {
            warehouse = props.getProperty("paimon.catalog.warehouse");
            if (warehouse == null) {
                throw new RuntimeException("paimon.catalog.warehouse must be configured in pixels-client.properties");
            }
            tableName = props.getProperty("paimon.table.name");
            if (tableName == null) {
                throw new RuntimeException("paimon.table.name must be configured in pixels-client.properties");
            }

            // Create Paimon Table
            Options options = new Options();
            options.set("warehouse", warehouse);
            
            // S3 Config for Paimon
            if (props.containsKey("s3.access-key")) options.set("s3.access-key", props.getProperty("s3.access-key"));
            if (props.containsKey("s3.secret-key")) options.set("s3.secret-key", props.getProperty("s3.secret-key"));
            if (props.containsKey("s3.endpoint")) options.set("s3.endpoint", props.getProperty("s3.endpoint"));
            if (props.containsKey("s3.path.style.access")) options.set("s3.path.style.access", props.getProperty("s3.path.style.access"));

            org.apache.paimon.catalog.Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            try {
                catalog.createDatabase("default", true);
            } catch (org.apache.paimon.catalog.Catalog.DatabaseAlreadyExistException e) {
                // Ignore
            }
            
            Identifier id = Identifier.create("default", tableName);
            if (catalog.tableExists(id)) {
                catalog.dropTable(id, true);
            }

            Schema.Builder schemaBuilder = Schema.newBuilder();
            schemaBuilder.column("id", DataTypes.INT());
            schemaBuilder.column("data", DataTypes.STRING());
            schemaBuilder.primaryKey("id");
            catalog.createTable(id, schemaBuilder.build(), false);
        } else {
            warehouse = props.getProperty("iceberg.catalog.warehouse");
            if (warehouse == null) {
                throw new RuntimeException("iceberg.catalog.warehouse must be configured in pixels-client.properties");
            }
            tableName = props.getProperty("iceberg.table.name");
             if (tableName == null) {
                throw new RuntimeException("iceberg.table.name must be configured in pixels-client.properties");
            }

            // Create Iceberg Table
            Configuration hadoopConf = new Configuration();
            // S3 Config for Hadoop
            if (props.containsKey("s3.access-key")) hadoopConf.set("fs.s3a.access.key", props.getProperty("s3.access-key"));
            if (props.containsKey("s3.secret-key")) hadoopConf.set("fs.s3a.secret.key", props.getProperty("s3.secret-key"));
            if (props.containsKey("s3.endpoint")) hadoopConf.set("fs.s3a.endpoint", props.getProperty("s3.endpoint"));
            if (props.containsKey("s3.path.style.access")) hadoopConf.set("fs.s3a.path.style.access", props.getProperty("s3.path.style.access"));

            String catalogType = props.getProperty("iceberg.catalog.type", "hadoop");
            CatalogLoader loader;
            Map<String, String> catalogProps = new HashMap<>();
            catalogProps.put("warehouse", warehouse);

            if ("glue".equalsIgnoreCase(catalogType)) {
                // AWS Glue Catalog
                catalogProps.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
                catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                
                // Optional: Specify AWS region
                if (props.containsKey("iceberg.catalog.glue.region")) {
                    catalogProps.put("glue.region", props.getProperty("iceberg.catalog.glue.region"));
                }
                
                loader = CatalogLoader.custom("glue_catalog", catalogProps, hadoopConf, 
                    "org.apache.iceberg.aws.glue.GlueCatalog");
            } else if ("rest".equalsIgnoreCase(catalogType)) {
                catalogProps.put("uri", props.getProperty("iceberg.catalog.uri"));
                catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                if (props.containsKey("iceberg.catalog.credential")) catalogProps.put("credential", props.getProperty("iceberg.catalog.credential"));
                
                // Pass S3 props
                if (props.containsKey("s3.endpoint")) catalogProps.put("s3.endpoint", props.getProperty("s3.endpoint"));
                if (props.containsKey("s3.access-key")) catalogProps.put("s3.access-key-id", props.getProperty("s3.access-key"));
                if (props.containsKey("s3.secret-key")) catalogProps.put("s3.secret-access-key", props.getProperty("s3.secret-key"));
                if (props.containsKey("s3.path.style.access")) catalogProps.put("s3.path-style-access", props.getProperty("s3.path.style.access"));

                loader = CatalogLoader.custom("rest_catalog", catalogProps, hadoopConf, "org.apache.iceberg.rest.RESTCatalog");
            } else {
                loader = CatalogLoader.hadoop("hadoop_catalog", hadoopConf, catalogProps);
            }

            Catalog catalog = loader.loadCatalog();
            TableIdentifier name = TableIdentifier.of("default", tableName);
            
            if (catalog.tableExists(name)) {
                catalog.dropTable(name);
            }

            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "data", Types.StringType.get())
            );
            catalog.createTable(name, schema, PartitionSpec.unpartitioned());
        }

        System.out.println("Submitting Writer Job (" + sinkType + ")...");
        
        // 2. Start Writer Job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);
        
        PixelsFlinkApp.buildJob(env, props);
        JobClient jobClient = env.executeAsync("Writer Job");

        // 3. Poll for Data Verification
        try {
            verifyDataWithTimeout(sinkType, props, tableName, 60000); // Increased timeout for S3
        } finally {
            // 4. Cancel Writer Job
            try {
                jobClient.cancel().get();
                System.out.println("Writer Job Cancelled.");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void verifyDataWithTimeout(String sinkType, Properties props, String tableName, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<String> rows = new ArrayList<>();
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // Register Catalog Once
        if ("paimon".equals(sinkType)) {
            String warehouse = props.getProperty("paimon.catalog.warehouse");
            // Parse database from table name if present
            String dbName = "default";
            if (tableName.contains(".")) {
                dbName = tableName.split("\\.")[0];
            }

            StringBuilder ddl = new StringBuilder();
            ddl.append("CREATE CATALOG paimon_verify WITH (");
            ddl.append("'type'='paimon', ");
            ddl.append("'warehouse'='").append(warehouse).append("'");
            // Add S3 props if present
            if (props.containsKey("s3.access-key")) ddl.append(", 's3.access-key'='").append(props.getProperty("s3.access-key")).append("'");
            if (props.containsKey("s3.secret-key")) ddl.append(", 's3.secret-key'='").append(props.getProperty("s3.secret-key")).append("'");
            if (props.containsKey("s3.endpoint")) ddl.append(", 's3.endpoint'='").append(props.getProperty("s3.endpoint")).append("'");
            if (props.containsKey("s3.path.style.access")) ddl.append(", 's3.path.style.access'='").append(props.getProperty("s3.path.style.access")).append("'");
            ddl.append(")");
            
            tEnv.executeSql(ddl.toString());
            tEnv.executeSql("USE CATALOG paimon_verify");
            if (!"default".equals(dbName)) {
                 tEnv.executeSql("USE " + dbName);
            }
        } else {
            String warehouse = props.getProperty("iceberg.catalog.warehouse");
            String catalogType = props.getProperty("iceberg.catalog.type", "hadoop");
            
            // Parse database from table name if present
            String dbName = "default";
            if (tableName.contains(".")) {
                dbName = tableName.split("\\.")[0];
            }

            StringBuilder ddl = new StringBuilder();
            ddl.append("CREATE CATALOG iceberg_verify WITH (");
            ddl.append("'type'='iceberg', ");
            
            if ("glue".equalsIgnoreCase(catalogType)) {
                ddl.append("'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog', ");
                ddl.append("'io-impl'='org.apache.iceberg.aws.s3.S3FileIO', ");
                
                // Optional: AWS region
                if (props.containsKey("iceberg.catalog.glue.region")) {
                    ddl.append("'glue.region'='").append(props.getProperty("iceberg.catalog.glue.region")).append("', ");
                }
            } else if ("rest".equalsIgnoreCase(catalogType)) {
                ddl.append("'catalog-type'='rest', ");
                ddl.append("'uri'='").append(props.getProperty("iceberg.catalog.uri")).append("', ");
                if (props.containsKey("iceberg.catalog.credential")) {
                    ddl.append("'credential'='").append(props.getProperty("iceberg.catalog.credential")).append("', ");
                }
                // S3 FileIO props need to be passed for REST too usually, depending on Flink connector version
                // Typically Flink connector passes properties to catalog
                ddl.append("'io-impl'='org.apache.iceberg.aws.s3.S3FileIO', ");
                if (props.containsKey("s3.endpoint")) ddl.append("'s3.endpoint'='").append(props.getProperty("s3.endpoint")).append("', ");
                if (props.containsKey("s3.access-key")) ddl.append("'s3.access-key-id'='").append(props.getProperty("s3.access-key")).append("', ");
                if (props.containsKey("s3.secret-key")) ddl.append("'s3.secret-access-key'='").append(props.getProperty("s3.secret-key")).append("', ");
                if (props.containsKey("s3.path.style.access")) ddl.append("'s3.path-style-access'='").append(props.getProperty("s3.path.style.access")).append("', ");
            } else {
                ddl.append("'catalog-type'='hadoop', ");
                // For Hadoop catalog, s3 props are picked up from flink-conf/hadoop-conf, or we can pass them as properties
                // Iceberg Flink connector might not support passing arbitrary hadoop conf via properties in DDL easily unless prefixed?
                // Actually standard Iceberg properties work.
                if (props.containsKey("s3.access-key")) ddl.append("'fs.s3a.access.key'='").append(props.getProperty("s3.access-key")).append("', ");
                if (props.containsKey("s3.secret-key")) ddl.append("'fs.s3a.secret.key'='").append(props.getProperty("s3.secret-key")).append("', ");
                if (props.containsKey("s3.endpoint")) ddl.append("'fs.s3a.endpoint'='").append(props.getProperty("s3.endpoint")).append("', ");
                if (props.containsKey("s3.path.style.access")) ddl.append("'fs.s3a.path.style.access'='").append(props.getProperty("s3.path.style.access")).append("', ");
            }
            
            ddl.append("'warehouse'='").append(warehouse).append("'");
            ddl.append(")");

            tEnv.executeSql(ddl.toString());
            tEnv.executeSql("USE CATALOG iceberg_verify");
        }

        while (System.currentTimeMillis() < deadline) {
            rows.clear();
            System.out.println("Polling verification data...");
            
            // Query Data
            try {
                TableResult readResult = tEnv.executeSql("SELECT id, data FROM " + tableName);
                try (CloseableIterator<Row> it = readResult.collect()) {
                    while (it.hasNext()) {
                        Row row = it.next();
                        rows.add(row.getField(0) + "," + row.getField(1));
                    }
                }
            } catch (Exception e) {
                // Table might not exist yet
                System.out.println("Query failed (table might not exist yet): " + e.getMessage());
            }

            // Check conditions
            boolean id2Exists = rows.contains("2,val2");
            boolean id1Updated = rows.contains("1,val1_updated");
            
            if (id2Exists && id1Updated && rows.size() == 2) {
                System.out.println("Verification Successful!");
                return;
            }
            
            Thread.sleep(2000); // Slower poll for S3
        }
        
        // If we fall through here, verification failed
        System.out.println("Verification Timed Out. Last read rows: " + rows);
        assertTrue("Should contain ID 2", rows.contains("2,val2"));
        assertTrue("Should contain updated ID 1", rows.contains("1,val1_updated"));
        assertEquals("Should have 2 records", 2, rows.size());
    }

    private void prepareData() {
        responseQueue.clear();
        // Insert ID=1
        responseQueue.add(PollResponse.newBuilder().addEvents(
                RowRecord.newBuilder()
                        .setOp(OperationType.INSERT)
                        .setAfter(createRowValue("1", "val1"))
                        .build()
        ).build());
        
        // Insert ID=2
        responseQueue.add(PollResponse.newBuilder().addEvents(
                RowRecord.newBuilder()
                        .setOp(OperationType.INSERT)
                        .setAfter(createRowValue("2", "val2"))
                        .build()
        ).build());
        
        // Update ID=1 -> val1_updated
        responseQueue.add(PollResponse.newBuilder().addEvents(
                RowRecord.newBuilder()
                        .setOp(OperationType.UPDATE)
                        .setBefore(createRowValue("1", "val1"))
                        .setAfter(createRowValue("1", "val1_updated"))
                        .build()
        ).build());
    }

    private RowValue createRowValue(String... values) {
        RowValue.Builder builder = RowValue.newBuilder();
        for (String val : values) {
            builder.addValues(ColumnValue.newBuilder()
                    .setValue(ByteString.copyFromUtf8(val))
                    .build());
        }
        return builder.build();
    }

    static class MockPixelsPollingService extends PixelsPollingServiceGrpc.PixelsPollingServiceImplBase {
        @Override
        public void pollEvents(PollRequest request, StreamObserver<PollResponse> responseObserver) {
            PollResponse response = responseQueue.poll();
            if (response == null) {
                response = PollResponse.newBuilder().build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
