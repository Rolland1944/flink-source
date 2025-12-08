package io.pixelsdb.pixels.flink;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.pixelsdb.pixels.sink.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.PixelsPollingServiceProto.PollRequest;
import io.pixelsdb.pixels.sink.PixelsPollingServiceProto.PollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class PixelsRpcClient implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PixelsRpcClient.class);

    static {
        // Force gRPC to use NIO transport to avoid Invalid argument errors with native Epoll on some Linux environments
        System.setProperty("io.grpc.netty.shaded.io.netty.transport.noNative", "true");
    }

    private final ManagedChannel channel;
    private final PixelsPollingServiceGrpc.PixelsPollingServiceBlockingStub blockingStub;

    public PixelsRpcClient(String host, int port) {
        // Use NettyChannelBuilder with explicit InetSocketAddress to avoid UnsupportedAddressTypeException
        // when underlying transport tries to use unsupported address types (e.g. from proxy or weird resolution)
        this(NettyChannelBuilder.forAddress(new InetSocketAddress(host, port))
                .usePlaintext()
                .build());
    }

    PixelsRpcClient(ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = PixelsPollingServiceGrpc.newBlockingStub(channel);
    }

    public PollResponse pollEvents(String schemaName, String tableName) {
        PollRequest request = PollRequest.newBuilder()
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .build();
        try {
            return blockingStub.pollEvents(request);
        } catch (Exception e) {
            LOG.error("RPC call failed: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted during shutdown", e);
            Thread.currentThread().interrupt();
        }
    }
}
