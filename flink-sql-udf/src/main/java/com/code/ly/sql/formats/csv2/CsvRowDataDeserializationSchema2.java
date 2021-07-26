/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.code.ly.sql.formats.csv2;

import com.code.ly.sql.util.StringUtil;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.csv.CsvRowSchemaConverter;
import org.apache.flink.formats.csv.CsvToRowDataConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Deserialization schema from CSV to Flink Table & SQL internal data structures.
 *
 * <p>Deserializes a <code>byte[]</code> message as a {@link JsonNode} and converts it to {@link
 * RowData}.
 *
 * <p>Failure during deserialization are forwarded as wrapped {@link IOException}s.
 */
@Internal
public final class CsvRowDataDeserializationSchema2 implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** Type information describing the result type. */
    private final TypeInformation<RowData> resultTypeInfo;

    /** Runtime instance that performs the actual work. */
    private final CsvToRowDataConverters.CsvToRowDataConverter runtimeConverter;

    /** Schema describing the input CSV data. */
    private final CsvSchema csvSchema;

    /** Object reader used to read rows. It is configured by {@link CsvSchema}. */
    private final ObjectReader objectReader;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    private final String secondFieldDelimiter;

    private final String discardPrefixFieldDelimiter;

    private final boolean ignoreTrailingUnmappable;

    private CsvRowDataDeserializationSchema2(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            CsvSchema csvSchema,
            boolean ignoreParseErrors,
            String secondFieldDelimiter,
            String discardPrefixFieldDelimiter,
            boolean ignoreTrailingUnmappable) {
        this.resultTypeInfo = resultTypeInfo;
        this.runtimeConverter =
                new CsvToRowDataConverters(ignoreParseErrors).createRowConverter(rowType, true);
        this.csvSchema = CsvRowSchemaConverter.convert(rowType);
        this.objectReader = new CsvMapper().readerFor(JsonNode.class).with(csvSchema);
        this.ignoreParseErrors = ignoreParseErrors;
        this.secondFieldDelimiter = secondFieldDelimiter;
        this.discardPrefixFieldDelimiter = discardPrefixFieldDelimiter;
        this.ignoreTrailingUnmappable = ignoreTrailingUnmappable;

        setFactoryFeatures();
    }

    /** entry for extra feature set **/
    public void setFactoryFeatures() {
        if(ignoreTrailingUnmappable) {
            CsvFactory csvFactory = (CsvFactory) this.objectReader.getJsonFactory();
            csvFactory.enable(CsvParser.Feature.IGNORE_TRAILING_UNMAPPABLE);
        }
    }



    /** A builder for creating a {@link CsvRowDataDeserializationSchema2}. */
    @Internal
    public static class Builder {

        private final RowType rowType;
        private final TypeInformation<RowData> resultTypeInfo;
        private CsvSchema csvSchema;
        private boolean ignoreParseErrors;
        private String secondFieldDelimiter;
        private String discardPrefixField;
        private boolean ignoreTrailingUnmappable;

        /**
         * Creates a CSV deserialization schema for the given {@link TypeInformation} with optional
         * parameters.
         */
        public Builder(RowType rowType, TypeInformation<RowData> resultTypeInfo) {
            Preconditions.checkNotNull(rowType, "RowType must not be null.");
            Preconditions.checkNotNull(resultTypeInfo, "Result type information must not be null.");
            this.rowType = rowType;
            this.resultTypeInfo = resultTypeInfo;
            this.csvSchema = CsvRowSchemaConverter.convert(rowType);
        }

        public Builder setFieldDelimiter(char delimiter) {
            this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(delimiter).build();
            return this;
        }

        public Builder setAllowComments(boolean allowComments) {
            this.csvSchema = this.csvSchema.rebuild().setAllowComments(allowComments).build();
            return this;
        }

        public Builder setArrayElementDelimiter(String delimiter) {
            Preconditions.checkNotNull(delimiter, "Array element delimiter must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
            return this;
        }

        public Builder setQuoteCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
            return this;
        }

        public Builder setEscapeCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
            return this;
        }

        public Builder setNullLiteral(String nullLiteral) {
            Preconditions.checkNotNull(nullLiteral, "Null literal must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setNullValue(nullLiteral).build();
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public Builder setSecondFieldDelimiter(String secondFieldDelimiter) {
            this.secondFieldDelimiter = secondFieldDelimiter;
            return this;
        }

        public Builder setDiscardPrefixField(String discardPrefixField) {
            this.discardPrefixField = discardPrefixField;
            return this;
        }

        public Builder setIgnoreTrailingUnmappable(boolean IgnoreTrailingUnmappable){
            this.ignoreTrailingUnmappable = ignoreTrailingUnmappable;
            return this;
        }

        public CsvRowDataDeserializationSchema2 build() {
            return new CsvRowDataDeserializationSchema2(
                    rowType, resultTypeInfo, csvSchema, ignoreParseErrors, secondFieldDelimiter, discardPrefixField, ignoreTrailingUnmappable);
        }
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {

            if(StringUtil.isNotBlank(discardPrefixFieldDelimiter)) {
                String[] strs =  new String(message).split(discardPrefixFieldDelimiter, -1);
                if (strs.length > 1) {
                    message = strs[1].getBytes();
                }
            }

            final JsonNode root = objectReader.readValue(message);
            return (RowData) runtimeConverter.convert(root);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    "Failed to deserialize CSV row '" + new String(message) + "'.", t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
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
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        final CsvRowDataDeserializationSchema2 that = (CsvRowDataDeserializationSchema2) o;
        final CsvSchema otherSchema = that.csvSchema;

        return resultTypeInfo.equals(that.resultTypeInfo)
                && ignoreParseErrors == that.ignoreParseErrors
                && secondFieldDelimiter == that.secondFieldDelimiter
                && discardPrefixFieldDelimiter == that.discardPrefixFieldDelimiter
                && csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator()
                && csvSchema.allowsComments() == otherSchema.allowsComments()
                && csvSchema
                        .getArrayElementSeparator()
                        .equals(otherSchema.getArrayElementSeparator())
                && csvSchema.getQuoteChar() == otherSchema.getQuoteChar()
                && csvSchema.getEscapeChar() == otherSchema.getEscapeChar()
                && Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                resultTypeInfo,
                ignoreParseErrors,
                secondFieldDelimiter,
                discardPrefixFieldDelimiter,
                csvSchema.getColumnSeparator(),
                csvSchema.allowsComments(),
                csvSchema.getArrayElementSeparator(),
                csvSchema.getQuoteChar(),
                csvSchema.getEscapeChar(),
                csvSchema.getNullValue());
    }
}
