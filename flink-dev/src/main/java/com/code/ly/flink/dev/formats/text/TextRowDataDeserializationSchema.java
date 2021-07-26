/*
package com.code.ly.flink.dev.formats.text;

import com.code.ly.flink.dev.util.CheckConditions;
import com.code.ly.flink.dev.util.StringUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.util.Preconditions.checkNotNull;

*/
/**
 * Deserialization schema from String to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a String and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *//*

public class TextRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    */
/** Flag indicating whether to fail if a field is missing. *//*

    private final boolean failOnMissingField;

    */
/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). *//*

    private final boolean ignoreParseErrors;

    */
/** split word used to split string **//*

    private final String fieldSplit;

    */
/** this config's format is specified like a=1,b=2,c=3 **//*

    private final String fieldIndex;

    private final static String fieldIndexSplit = ",";

    */
/** reflection of field to index **//*

   private final Map<String,Integer> fieldIndexMap;


    */
/** TypeInformation of the produced {@link RowData}. **//*

    private final TypeInformation<RowData> resultTypeInfo;

    */
/**
     * Runtime converter that converts String into
     * objects of Flink SQL internal data structures. **//*

    private final DeserializationRuntimeConverter runtimeConverter;

    */
/** Timestamp format specification which is used to parse timestamp. *//*

    private final TimestampFormat timestampFormat;

    public TextRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            String indexSplit,
            String fieldIndex,
            TimestampFormat timestampFormat) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.fieldSplit = indexSplit;
        this.fieldIndex = fieldIndex;
        this.fieldIndexMap = parseFieldIndex(fieldIndex);
        this.runtimeConverter = createRowConverter(checkNotNull(rowType));
        this.timestampFormat = timestampFormat;
//        Map<String,Integer> fieldIndexMap= new HashMap<>();
//        fieldIndex.split()
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        try {
            final String str = new String(bytes);
            return (RowData) runtimeConverter.convert(str);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(format("Failed to deserialize JSON '%s'.", new String(bytes)), t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TextRowDataDeserializationSchema that = (TextRowDataDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField &&
                ignoreParseErrors == that.ignoreParseErrors &&
                resultTypeInfo.equals(that.resultTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOnMissingField, ignoreParseErrors, resultTypeInfo);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    */
/**
     * Runtime converter that converts String into objects of Flink Table & SQL
     * internal data structures.
     *//*

    @FunctionalInterface
    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(String str);
    }

    */
/**
     * Creates a runtime converter which is null safe.
     *//*

    private DeserializationRuntimeConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    */
/**
     * Creates a runtime converter which assuming input object is not null.
     *//*

    private DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return str -> null;
            case BOOLEAN:
                return str -> Boolean.parseBoolean(str);
            case TINYINT:
                return str -> Byte.parseByte(str.trim());
            case SMALLINT:
                return str -> Short.parseShort(str.trim());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return str -> Integer.parseInt(str.trim());
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return str -> Long.parseLong(str.trim());
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case FLOAT:
                return str -> Float.parseFloat(str.trim());
            case DOUBLE:
                return str -> Double.parseDouble(str.trim());
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return str -> str.getBytes(StandardCharsets.UTF_8);
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                throw new UnsupportedOperationException("Unsupported type: " + type);
            case MAP:
            case MULTISET:
                throw new UnsupportedOperationException("Unsupported type: " + type);
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }


    private StringData convertToString(String s) {
        return StringData.fromString(s);
    }

    private int convertToDate(String s) {
        LocalDate date = ISO_LOCAL_DATE.parse(s).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    private int convertToTime(String s) {
        TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(s);
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

        // get number of milliseconds of the day
        return localTime.toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(String s) {
        TemporalAccessor parsedTimestamp;
        switch (timestampFormat){
            case SQL:
                //parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(s);
                break;
            case ISO_8601:
                //parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(s);
                break;
            default:
                throw new TableException(String.format("Unsupported timestamp format '%s'. Validator should have checked that.", timestampFormat));
        }
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return str -> {
            BigDecimal bigDecimal;
            bigDecimal = new BigDecimal(str.trim());
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }


    private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
        final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .map(this::createConverter)
                .toArray(DeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return str -> {
            String[] fields = str.split(fieldSplit, -1);
            int arity = fieldNames.length;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                int index = fieldIndexMap.get(fieldName);
                //String field = str
                //Object convertedField = convertField(fieldConverters[i], fieldName, field);
                //row.setField(i, convertedField);
            }
            return row;
        };
    }
//
//    private Object convertField(
//            DeserializationRuntimeConverter fieldConverter,
//            String fieldName,
//            JsonNode field) {
//        if (field == null) {
//            if (failOnMissingField) {
//                throw new JsonParseException(
//                        "Could not find field with name '" + fieldName + "'.");
//            } else {
//                return null;
//            }
//        } else {
//            return fieldConverter.convert(field);
//        }
//    }

    private DeserializationRuntimeConverter wrapIntoNullableConverter(
            DeserializationRuntimeConverter converter) {
        return str -> {
            if (str == null) {
                return null;
            }
            try {
                return converter.convert(str);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }

    private Map<String, Integer> parseFieldIndex(String s) {
        Map<String, Integer> fieldIndexMap = new HashMap<>();
        String[] fieldIndexArray = s.split(fieldIndexSplit,-1);
        for(String fieldIndexStr: fieldIndexArray) {
            CheckConditions.checkState(StringUtil.isNotBlank(fieldIndexStr), "fieldIndex shouldn't be null or empty");
            String[] fieldIndex = fieldIndexStr.split("=", -1);
            CheckConditions.checkState(fieldIndex.length == 2,"you might miss '=' for definition of field and index");
            CheckConditions.checkState(StringUtil.isNumeric(fieldIndex[1]) && Integer.parseInt(fieldIndex[1]) >=0, "the index of field should be number and >= 0");
            fieldIndexMap.put(fieldIndex[0], Integer.parseInt(fieldIndex[1]));
        }

        return fieldIndexMap;
    }
}
*/
