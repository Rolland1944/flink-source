package io.pixelsdb.pixels.flink;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.ColumnValue;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.OperationType;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.PollResponse;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.RowRecord;
import io.pixelsdb.pixels.sink.rpc.PixelsPollingServiceProto.RowValue;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

public class PixelsRpcSource extends RichSourceFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(PixelsRpcSource.class);

    private final String host;
    private final int port;
    private final String schemaName;
    private final String tableName;
    private final RowType rowType;

    private transient PixelsRpcClient client;
    private volatile boolean isRunning = true;

    public PixelsRpcSource(Properties props, RowType rowType) {
        this.host = props.getProperty("pixels.server.host", "").trim();
        this.port = Integer.parseInt(props.getProperty("pixels.server.port", "0").trim());
        this.schemaName = props.getProperty("schema.name", "public");
        this.tableName = props.getProperty("table.name");
        this.rowType = rowType;
        LOG.info("Configured PixelsRpcSource with host='{}', port={}", host, port);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new PixelsRpcClient(host, port);
        LOG.info("PixelsRpcSource started for table {}.{} at {}:{}", schemaName, tableName, host, port);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (isRunning) {
            try {
                // Poll events
                PollResponse response = client.pollEvents(schemaName, tableName);
                List<RowRecord> events = response.getEventsList();

                if (!events.isEmpty()) {
                    LOG.info("PixelsRpcSource received {} events", events.size());
                }

                for (RowRecord event : events) {
                    processEvent(event, ctx);
                }
            } catch (Exception e) {
                LOG.error("Error during polling", e);
            }
        }
    }

    private void processEvent(RowRecord event, SourceContext<RowData> ctx) {
        OperationType op = event.getOp();
        switch (op) {
            case INSERT:
            case SNAPSHOT:
                if (event.hasAfter()) {
                    ctx.collect(convert(event.getAfter(), RowKind.INSERT));
                }
                break;
            case UPDATE:
                if (event.hasBefore()) {
                    ctx.collect(convert(event.getBefore(), RowKind.UPDATE_BEFORE));
                }
                if (event.hasAfter()) {
                    ctx.collect(convert(event.getAfter(), RowKind.UPDATE_AFTER));
                }
                break;
            case DELETE:
                if (event.hasBefore()) {
                    ctx.collect(convert(event.getBefore(), RowKind.DELETE));
                }
                break;
            default:
                LOG.warn("Unknown operation type: {}", op);
        }
    }

    private RowData convert(RowValue rowValue, RowKind kind) {
        List<ColumnValue> values = rowValue.getValuesList();
        int arity = rowType.getFieldCount();
        GenericRowData row = new GenericRowData(kind, arity);
        List<LogicalType> fieldTypes = rowType.getChildren();

        // Assuming the order of values in RowValue matches the schema definition
        for (int i = 0; i < arity; i++) {
            if (i < values.size()) {
                ByteString byteString = values.get(i).getValue();
                LogicalType type = fieldTypes.get(i);
                row.setField(i, parseValue(byteString, type));
            } else {
                // Missing value, set null or handle error
                row.setField(i, null);
            }
        }
        return row;
    }

    private Object parseValue(ByteString byteString, LogicalType type) {
        if (byteString == null || byteString.isEmpty()) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                return byteString.toByteArray();
            default:
                // Continue to string parsing
        }

        String value = byteString.toStringUtf8();
        
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(value);
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return DecimalData.fromBigDecimal(new BigDecimal(value), decimalType.getPrecision(), decimalType.getScale());
            case DATE:
                return (int) LocalDate.parse(value).toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return (int) (LocalTime.parse(value).toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // Assuming format yyyy-MM-dd HH:mm:ss or ISO
                try {
                    return TimestampData.fromLocalDateTime(LocalDateTime.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                } catch (Exception e) {
                    // Fallback to ISO
                    return TimestampData.fromLocalDateTime(LocalDateTime.parse(value));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromInstant(ZonedDateTime.parse(value).toInstant());
            default:
                return StringData.fromString(value); // Fallback
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
        super.close();
    }
}
