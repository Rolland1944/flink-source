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
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PixelsSinkTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

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
    public void testPaimonSink() throws Exception {
        runTest("paimon");
    }

    @Test
    public void testIcebergSink() throws Exception {
        runTest("iceberg");
    }

    private void runTest(String sinkType) throws Exception {
        // Load Properties
        Properties props = new Properties();
        try (InputStream input = PixelsFlinkApp.class.getClassLoader().getResourceAsStream("pixels-client.properties")) {
            if (input != null) {
                props.load(input);
            } else {
                System.out.println("Sorry, unable to find pixels-client.properties");
                return;
            }
        }

        // Push data
        responseQueue.add(createInsertEvent());
        responseQueue.add(createUpdateEvent());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Enable Checkpoint for Sinks
        env.enableCheckpointing(1000);

        // Overwrite/Ensure essential properties
        props.setProperty("pixels.server.host", "localhost");
        props.setProperty("pixels.server.port", String.valueOf(port));
        props.setProperty("sink.type", sinkType);
        
        String warehouse;
        if ("paimon".equals(sinkType)) {
            warehouse = props.getProperty("paimon.catalog.warehouse");
            if (warehouse == null) throw new RuntimeException("paimon.catalog.warehouse is not set in properties");

            // Create Paimon Table
            Options options = new Options();
            options.set("warehouse", warehouse);
            
            // Add S3 options
            if (props.containsKey("s3.access-key")) options.set("s3.access-key", props.getProperty("s3.access-key"));
            if (props.containsKey("s3.secret-key")) options.set("s3.secret-key", props.getProperty("s3.secret-key"));
            if (props.containsKey("s3.endpoint")) options.set("s3.endpoint", props.getProperty("s3.endpoint"));
            if (props.containsKey("s3.path.style.access")) options.set("s3.path.style.access", props.getProperty("s3.path.style.access"));

            // Enable S3 support for Hadoop FileSystem
            options.set("hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            options.set("hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

            org.apache.paimon.catalog.Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            try {
                catalog.createDatabase("default", true);
            } catch (org.apache.paimon.catalog.Catalog.DatabaseAlreadyExistException e) {
                // Ignore
            }
            Schema.Builder schemaBuilder = Schema.newBuilder();
            schemaBuilder.column("id", DataTypes.INT());
            schemaBuilder.column("data", DataTypes.STRING());
            schemaBuilder.primaryKey("id");
            try {
                catalog.createTable(Identifier.create("default", props.getProperty("paimon.table.name", "paimon_tbl")), schemaBuilder.build(), false);
            } catch (org.apache.paimon.catalog.Catalog.TableAlreadyExistException e) {
                // Ignore
            }
        } else {
            warehouse = props.getProperty("iceberg.catalog.warehouse");
            if (warehouse == null) throw new RuntimeException("iceberg.catalog.warehouse is not set in properties");
            
            props.setProperty("iceberg.catalog.type", "hadoop");

            // Create Iceberg Table
            Configuration hadoopConf = new Configuration();
            // Add S3 options
            if (props.containsKey("s3.access-key")) hadoopConf.set("fs.s3a.access.key", props.getProperty("s3.access-key"));
            if (props.containsKey("s3.secret-key")) hadoopConf.set("fs.s3a.secret.key", props.getProperty("s3.secret-key"));
            if (props.containsKey("s3.endpoint")) hadoopConf.set("fs.s3a.endpoint", props.getProperty("s3.endpoint"));
            if (props.containsKey("s3.path.style.access")) hadoopConf.set("fs.s3a.path.style.access", props.getProperty("s3.path.style.access"));
            
            // Enable S3 support
            hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

            HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "data", Types.StringType.get())
            );
            TableIdentifier name = TableIdentifier.of("default", props.getProperty("iceberg.table.name", "iceberg_tbl"));
            if (!catalog.tableExists(name)) {
                catalog.createTable(name, schema, PartitionSpec.unpartitioned());
            }
        }

        // Build job graph
        PixelsFlinkApp.buildJob(env, props);
        
        // Run job asynchronously
        JobClient jobClient = env.executeAsync("Test Sink Job");
        
        // Wait a bit for job to run and process events
        Thread.sleep(5000);
        
        // Cancel job
        try {
            jobClient.cancel().get();
        } catch (Exception e) {
            // Expected
        }
    }

    private PollResponse createInsertEvent() {
        RowRecord event = RowRecord.newBuilder()
                .setOp(OperationType.INSERT)
                .setAfter(createRowValue("1", "insert"))
                .build();
        return PollResponse.newBuilder().addEvents(event).build();
    }

    private PollResponse createUpdateEvent() {
        RowRecord event = RowRecord.newBuilder()
                .setOp(OperationType.UPDATE)
                .setBefore(createRowValue("1", "insert"))
                .setAfter(createRowValue("1", "update"))
                .build();
        return PollResponse.newBuilder().addEvents(event).build();
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
