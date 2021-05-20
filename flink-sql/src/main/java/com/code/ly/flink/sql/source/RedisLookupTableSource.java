package com.code.ly.flink.sql.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

/**
 * @author
 * @link see https://github.com/lonelyGhostisdog/flinksql/blob/master/src/main/java/source/RedisLookupTableSource.java
 */
public class RedisLookupTableSource implements LookupableTableSource<Row>, StreamTableSource<Row> {
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String ip;
    private final int port;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    public RedisLookupTableSource(String[] fieldNames, TypeInformation[] fieldTypes, String ip, int port, long cacheMaxSize, long cacheExpireMs) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.ip = ip;
        this.port = port;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] strings) {
        return null;
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] strings) {
        return RedisLookupFunction.builder()
                .setFieldNames(fieldNames)
                .setFieldTypes(fieldTypes)
                .setIp(ip)
                .setPort(port)
                .setCacheMaxSize(cacheMaxSize)
                .setCacheExpireMs(cacheExpireMs)
                .build();
    }

    /**
     * 是否异步
     * @return
     */
    @Override
    public boolean isAsyncEnabled() {
        return true;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        throw new IllegalArgumentException("redis only support table/sql now");
    }

    /**
     * 表结构
     * @return
     */
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
                .build();
    }

    @Override
    public boolean isBounded() {
        return false;
    }

    @Override
    public DataType getProducedDataType() {
        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames));
    }
}
