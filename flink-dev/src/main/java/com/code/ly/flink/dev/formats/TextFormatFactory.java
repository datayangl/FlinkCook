package com.code.ly.flink.dev.formats;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Set;

import static com.code.ly.flink.dev.formats.text.TextOptions.*;
import static com.code.ly.flink.dev.formats.text.TextOptions.FAIL_ON_MISSING_FIELD;
import static com.code.ly.flink.dev.formats.text.TextOptions.IGNORE_PARSE_ERRORS;

public class TextFormatFactory implements
        DeserializationFormatFactory,
        SerializationFormatFactory {

    public static final String IDENTIFIER = "split-text";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);

        final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        final String fieldSplit = formatOptions.get(FIELD_SPLIT);
        final String fieldIndex = formatOptions.get(FIELD_INDEX);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType dataType) {
                final RowType rowType = (RowType) dataType.getLogicalType();
                //final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(dataType);
                return null;
//                return new TextRowDataDeserializationSchema(
//                        rowType,
//                        rowDataTypeInfo,
//                        failOnMissingField,
//                        ignoreParseErrors,
//                        fieldSplit,
//                        fieldIndex
//                );
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FIELD_INDEX);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(FIELD_SPLIT);
        options.add(FIELD_INDEX);
        return options;
    }


    // ------------------------------------------------------------------------
    //  Validation
    // ------------------------------------------------------------------------

    static void validateFormatOptions(ReadableConfig tableOptions) {
        boolean failOnMissingField = tableOptions.get(FAIL_ON_MISSING_FIELD);
        boolean ignoreParseErrors = tableOptions.get(IGNORE_PARSE_ERRORS);
        String indexSchema = tableOptions.get(FIELD_INDEX);
        if (ignoreParseErrors && failOnMissingField) {
            throw new ValidationException(FAIL_ON_MISSING_FIELD.key()
                    + " and "
                    + IGNORE_PARSE_ERRORS.key()
                    + " shouldn't both be true.");
        }

        if (indexSchema == null || "".equals(indexSchema)) {
            throw new ValidationException(FIELD_INDEX.key() + "shouldn' be null.");
        }
    }
}
