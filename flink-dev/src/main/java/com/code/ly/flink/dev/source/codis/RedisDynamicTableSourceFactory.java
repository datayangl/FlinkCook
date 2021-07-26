package com.code.ly.flink.dev.source.codis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.types.Row;

import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

public class RedisDynamicTableSourceFactory implements DynamicTableSourceFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        validateOptions(options);
        TableSchema schema = context.getCatalogTable().getSchema();
        return new RedisDynamicTableSource(options, schema);
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }


    void validateOptions(ReadableConfig options) {

    }
}
