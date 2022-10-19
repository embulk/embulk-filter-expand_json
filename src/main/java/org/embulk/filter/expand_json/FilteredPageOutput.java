/*
 * Copyright 2015 Takahiro Nakayama, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.filter.expand_json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.ReadContext;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.json.JsonParseException;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.embulk.filter.expand_json.ExpandJsonFilterPlugin.PluginTask;

public class FilteredPageOutput
    implements PageOutput
{
    private class ExpandedColumn
    {
        private final String key;
        private final Column column;
        private final String jsonPath;
        private final Optional<TimestampFormatter> timestampFormatter;

        ExpandedColumn(String key,
                       Column column,
                       String jsonPath,
                       Optional<TimestampFormatter> timestampFormatter)
        {
            this.key = key;
            this.column = column;
            this.jsonPath = jsonPath;
            this.timestampFormatter = timestampFormatter;
        }

        public String getKey()
        {
            return key;
        }

        public Column getColumn()
        {
            return column;
        }

        public String getJsonPath()
        {
            return jsonPath;
        }

        public Optional<TimestampFormatter> getTimestampFormatter()
        {
            return timestampFormatter;
        }
    }

    private class UnchangedColumn
    {
        private final String key;
        private final Column inputColumn;
        private final Column outputColumn;

        UnchangedColumn(String key, Column inputColumn, Column outputColumn)
        {
            this.key = key;
            this.inputColumn = inputColumn;
            this.outputColumn = outputColumn;
        }

        public String getKey()
        {
            return key;
        }

        public Column getInputColumn()
        {
            return inputColumn;
        }

        public Column getOutputColumn()
        {
            return outputColumn;
        }
    }

    // Copied from org.embulk.spi.time.TimestampParser.TimestampColumnOption for embulk-util-timestamp.
    private interface TimestampColumnOption
            extends Task {
        @Config("timezone")
        @ConfigDefault("null")
        Optional<String> getTimeZoneId();

        @Config("format")
        @ConfigDefault("null")
        Optional<String> getFormat();

        @Config("date")
        @ConfigDefault("null")
        Optional<String> getDate();
    }

    private static TimestampFormatter createTimestampFormatter(final PluginTask task,
                                                               final ColumnConfig columnConfig)
    {
        final ConfigMapper configMapper = ExpandJsonFilterPlugin.getConfigMapperFactory().createConfigMapper();
        final TimestampColumnOption columnOption = configMapper.map(columnConfig.getOption(), TimestampColumnOption.class);
        return TimestampFormatter.builder(columnOption.getFormat().orElse(task.getDefaultTimestampFormat()), true)
                .setDefaultZoneFromString(columnOption.getTimeZoneId().orElse(task.getDefaultTimeZoneId()))
                .setDefaultDateFromString(columnOption.getDate().orElse(task.getDefaultDate()))
                .build();
    }

    private static final Logger logger = LoggerFactory.getLogger(FilteredPageOutput.class);
    private final boolean stopOnInvalidRecord;
    private final boolean keepExpandingJsonColumn;
    private final List<UnchangedColumn> unchangedColumns;
    private final List<ExpandedColumn> expandedColumns;
    private final Column jsonColumn;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ParseContext parseContext;
    private final JsonParser jsonParser = new JsonParser();

    private List<ExpandedColumn> initializeExpandedColumns(PluginTask task,
                                                           Schema outputSchema)
    {
        final ArrayList<ExpandedColumn> expandedJsonColumnsBuilder = new ArrayList<>();
        for (Column outputColumn : outputSchema.getColumns()) {
            for (ColumnConfig expandedColumnConfig : task.getExpandedColumns()) {
                if (outputColumn.getName().equals(expandedColumnConfig.getName())) {

                    TimestampFormatter timestampFormatter = null;
                    if (Types.TIMESTAMP.equals(expandedColumnConfig.getType())) {
                        timestampFormatter = createTimestampFormatter(task, expandedColumnConfig);
                    }

                    ExpandedColumn expandedColumn = new ExpandedColumn(outputColumn.getName(),
                                                                       outputColumn,
                                                                       task.getRoot() + outputColumn.getName(),
                                                                       Optional.ofNullable(timestampFormatter));
                    expandedJsonColumnsBuilder.add(expandedColumn);
                }
            }
        }
        return Collections.unmodifiableList(expandedJsonColumnsBuilder);
    }

    private List<UnchangedColumn> initializeUnchangedColumns(Schema inputSchema,
                                                             Schema outputSchema,
                                                             Column excludeColumn)
    {
        final ArrayList<UnchangedColumn> unchangedColumnsBuilder = new ArrayList<>();
        for (Column outputColumn : outputSchema.getColumns()) {
            for (Column inputColumn : inputSchema.getColumns()) {
                if (inputColumn.getName().equals(outputColumn.getName()) &&
                        (!excludeColumn.getName().equals(outputColumn.getName()) || keepExpandingJsonColumn)) {

                    UnchangedColumn unchangedColumn = new UnchangedColumn(outputColumn.getName(),
                                                                          inputColumn,
                                                                          outputColumn);
                    unchangedColumnsBuilder.add(unchangedColumn);
                }
            }
        }
        return Collections.unmodifiableList(unchangedColumnsBuilder);
    }

    private Column initializeJsonColumn(PluginTask task, Schema inputSchema)
    {
        Column jsonColumn = null;
        for (Column column: inputSchema.getColumns()) {
            if (column.getName().contentEquals(task.getJsonColumnName())) {
                jsonColumn = column;
            }
        }
        return jsonColumn;
    }

    private ParseContext initializeParseContext()
    {
        Configuration conf = Configuration.defaultConfiguration();
        conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        conf = conf.addOptions(Option.SUPPRESS_EXCEPTIONS);
        return JsonPath.using(conf);
    }

    FilteredPageOutput(PluginTask task, Schema inputSchema, Schema outputSchema, PageOutput pageOutput)
    {
        this.stopOnInvalidRecord = task.getStopOnInvalidRecord();
        this.keepExpandingJsonColumn = task.getKeepExpandingJsonColumn();
        this.jsonColumn = initializeJsonColumn(task, inputSchema);
        this.unchangedColumns = initializeUnchangedColumns(inputSchema,
                                                           outputSchema,
                                                           jsonColumn);
        this.expandedColumns = initializeExpandedColumns(task,
                                                         outputSchema);

        this.pageReader = new PageReader(inputSchema);
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        this.parseContext = initializeParseContext();
    }

    @Override
    public void add(Page page)
    {
        pageReader.setPage(page);
        while (pageReader.nextRecord()) {
            try {
                setExpandedJsonColumns();
                setUnchangedColumns();
                pageBuilder.addRecord();
            }
            catch (DataException | JsonProcessingException e) {
                if (stopOnInvalidRecord) {
                    throw new DataException(String.format("Found an invalid record"), e);
                }
                logger.warn(String.format("Skipped an invalid record (%s)", e.getMessage()));
            }
        }
    }

    @Override
    public void finish()
    {
        pageBuilder.finish();
    }

    @Override
    public void close()
    {
        pageReader.close();
        pageBuilder.close();
    }


    private void setUnchangedColumns() {
        for (UnchangedColumn unchangedColumn : unchangedColumns) {
            Column inputColumn = unchangedColumn.getInputColumn();
            Column outputColumn = unchangedColumn.getOutputColumn();

            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(outputColumn);
                continue;
            }

            if (Types.STRING.equals(outputColumn.getType())) {
                pageBuilder.setString(outputColumn, pageReader.getString(inputColumn));
            }
            else if (Types.BOOLEAN.equals(outputColumn.getType())) {
                pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(inputColumn));
            }
            else if (Types.DOUBLE.equals(outputColumn.getType())) {
                pageBuilder.setDouble(outputColumn, pageReader.getDouble(inputColumn));
            }
            else if (Types.LONG.equals(outputColumn.getType())) {
                pageBuilder.setLong(outputColumn, pageReader.getLong(inputColumn));
            }
            else if (Types.TIMESTAMP.equals(outputColumn.getType())) {
                pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(inputColumn));
            }
            else { // Json type
                pageBuilder.setJson(outputColumn, pageReader.getJson(inputColumn));
            }
        }
    }

    private void setExpandedJsonColumns()
            throws JsonProcessingException
    {
        final Map<Value, Value> map;

        if (pageReader.isNull(jsonColumn)) {
            map = null;
        }
        else {
            final Value json;

            if (jsonColumn.getType().equals(Types.JSON)) {
                json = pageReader.getJson(jsonColumn);
            }
            else {
                json = jsonParser.parse(pageReader.getString(jsonColumn));
            }

            if (json == null) {
                map = null;
            } else {
                map = json.asMapValue().map();
            }
        }

        for (ExpandedColumn expandedJsonColumn: expandedColumns) {
            if (map == null) {
                pageBuilder.setNull(expandedJsonColumn.getColumn());
                continue;
            }

            final Value value = map.get(ValueFactory.newString(expandedJsonColumn.getKey()));
            if (value == null) {
                pageBuilder.setNull(expandedJsonColumn.getColumn());
                continue;
            }

            if (Types.STRING.equals(expandedJsonColumn.getColumn().getType())) {
                pageBuilder.setString(expandedJsonColumn.getColumn(), value.asStringValue().toString());
            }
            else if (Types.BOOLEAN.equals(expandedJsonColumn.getColumn().getType())) {
                pageBuilder.setBoolean(expandedJsonColumn.getColumn(), value.asBooleanValue().getBoolean());
            }
            else if (Types.DOUBLE.equals(expandedJsonColumn.getColumn().getType())) {
                try {
                    pageBuilder.setDouble(expandedJsonColumn.getColumn(), value.asFloatValue().toDouble());
                }
                catch (MessageTypeCastException e) {
                    throw new JsonValueInvalidException(String.format("Failed to parse '%s' as double", expandedJsonColumn.getKey()), e);
                }
            }
            else if (Types.LONG.equals(expandedJsonColumn.getColumn().getType())) {
                try {
                    pageBuilder.setLong(expandedJsonColumn.getColumn(), value.asIntegerValue().toLong());
                }
                catch (MessageTypeCastException e) {
                    // ad-hoc workaround for exponential notation
                    try {
                        pageBuilder.setLong(expandedJsonColumn.getColumn(), (long) value.asFloatValue().toDouble());
                    }
                    catch (MessageTypeCastException e2) {
                        throw new JsonValueInvalidException(String.format("Failed to parse '%s' as long", expandedJsonColumn.getKey()), e);
                    }
                }
            }
            else if (Types.TIMESTAMP.equals(expandedJsonColumn.getColumn().getType())) {
                if (expandedJsonColumn.getTimestampFormatter().isPresent()) {
                    TimestampFormatter formatter = expandedJsonColumn.getTimestampFormatter().get();
                    try {
                        pageBuilder.setTimestamp(expandedJsonColumn.getColumn(), Timestamp.ofInstant(formatter.parse(value.asStringValue().asString())));
                    }
                    catch (DateTimeParseException e) {
                        throw new JsonValueInvalidException(String.format("Failed to parse '%s' as timestamp", value.asStringValue().asString()), e);
                    }
                }
                else {
                    throw new RuntimeException("TimestampFormatter is absent for column:" + expandedJsonColumn.getKey());
                }
            }
            else if (Types.JSON.equals(expandedJsonColumn.getColumn().getType())) {
                try {
                    pageBuilder.setJson(expandedJsonColumn.getColumn(), value);
                }
                catch (JsonParseException e) {
                    throw new JsonValueInvalidException(String.format("Failed to parse '%s' as JSON", expandedJsonColumn.getKey()), e);
                }
            }
        }
    }

    private String convertJsonNodeAsString(Object value)
            throws JsonProcessingException
    {
        if (value == null) {
            return null;
        }
        else if (value instanceof List) {
            return objectMapper.writeValueAsString(value);
        }
        else if (value instanceof Map) {
            return objectMapper.writeValueAsString(value);
        }
        else if (value instanceof String) {
            return (String) value;
        }
        else {
            return String.valueOf(value);
        }
    }

    private class JsonValueInvalidException extends DataException
    {
        JsonValueInvalidException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}
