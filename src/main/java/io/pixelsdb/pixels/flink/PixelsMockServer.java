package io.pixelsdb.pixels.flink;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.sink.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.PixelsPollingServiceProto.ColumnValue;
import io.pixelsdb.pixels.sink.PixelsPollingServiceProto.OperationType;
import io.pixelsdb.pixels.sink.PixelsPollingServiceProto.PollRequest;
import io.pixelsdb.pixels.sink.PixelsPollingServiceProto.PollResponse;
import io.pixelsdb.pixels.sink.PixelsPollingServiceProto.RowRecord;
import io.pixelsdb.pixels.sink.PixelsPollingServiceProto.RowValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A standalone Mock Server that simulates PixelsDB Sink.
 * It listens on the port configured in pixels-client.properties and serves mock data via gRPC.
 */
public class PixelsMockServer {
    private static final Logger LOG = LoggerFactory.getLogger(PixelsMockServer.class);

    private Server server;

    private void start() throws IOException {
        // Load properties
        Properties props = new Properties();
        try (InputStream input = PixelsMockServer.class.getClassLoader()
                .getResourceAsStream("pixels-client.properties")) {
            if (input == null) {
                LOG.error("Sorry, unable to find pixels-client.properties");
                throw new IOException("pixels-client.properties not found");
            }
            props.load(input);
        }

        String portStr = props.getProperty("pixels.server.port", "50051").trim();
        int port = Integer.parseInt(portStr);

        server = ServerBuilder.forPort(port)
                .addService(new MockPixelsPollingService())
                .build()
                .start();

        LOG.info("Mock Pixels Server started, listening on port {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("*** shutting down gRPC server since JVM is shutting down");
            try {
                PixelsMockServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            LOG.info("*** server shut down");
        }));
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final PixelsMockServer server = new PixelsMockServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class MockPixelsPollingService extends PixelsPollingServiceGrpc.PixelsPollingServiceImplBase {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public void pollEvents(PollRequest request, StreamObserver<PollResponse> responseObserver) {
            LOG.info("Received poll request for table: {}.{}", request.getSchemaName(), request.getTableName());

            // Simulate generating data
            // We generate 1 event per poll call to simulate a stream
            long id = counter.incrementAndGet();
            String data = "mock_data_" + id;
            double price = 10.0 + (id * 0.5);

            // Construct row based on default schema: id:INT,data:STRING,price:DOUBLE
            RowRecord record = RowRecord.newBuilder()
                    .setOp(OperationType.INSERT)
                    .setAfter(createRowValue(String.valueOf(id), data, String.valueOf(price)))
                    .build();

            PollResponse response = PollResponse.newBuilder()
                    .addEvents(record)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        private RowValue createRowValue(String... values) {
            RowValue.Builder builder = RowValue.newBuilder();
            for (String val : values) {
                builder.addValues(ColumnValue.newBuilder()
                        .setValue(ByteString.copyFromUtf8(val)).build());
            }
            return builder.build();
        }
    }
}