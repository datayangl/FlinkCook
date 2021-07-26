package com.code.ly.flink.dev.source.codis;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.util.Preconditions;

public class RedisDynamicTableSource implements LookupTableSource {
    private final ReadableConfig options;
    private final TableSchema schema;

    public RedisDynamicTableSource(ReadableConfig options, TableSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        Preconditions.checkArgument(context.getKeys().length == 1 && context.getKeys()[0].length == 1, "Redis source only supports lookup by single key");

        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(options, schema);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Source";
    }
}
