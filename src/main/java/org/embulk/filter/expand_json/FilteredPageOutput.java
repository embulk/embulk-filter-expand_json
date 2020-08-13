package org.embulk.filter.expand_json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.ReadContext;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
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
import org.embulk.util.json.JsonParseException;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
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
        final ReadContext json;
        if (pageReader.isNull(jsonColumn)) {
            json = null;
        }
        else {
            String jsonObject;
            if (jsonColumn.getType().equals(Types.JSON)) {
                jsonObject = pageReader.getJson(jsonColumn).toJson(); // TODO could use Value object directly and optimize this code
            }
            else {
                jsonObject = pageReader.getString(jsonColumn);
            }

            if (jsonObject == null || jsonObject.isEmpty()) {
                json = null;
            } else {
                json = parseContext.parse(jsonObject);
            }
        }

        for (ExpandedColumn expandedJsonColumn: expandedColumns) {
            if (json == null) {
                pageBuilder.setNull(expandedJsonColumn.getColumn());
                continue;
            }

            Object value = json.read(expandedJsonColumn.getJsonPath());
            final String finalValue = convertJsonNodeAsString(value);
            if (finalValue == null) {
                pageBuilder.setNull(expandedJsonColumn.getColumn());
                continue;
            }

            if (Types.STRING.equals(expandedJsonColumn.getColumn().getType())) {
                pageBuilder.setString(expandedJsonColumn.getColumn(), finalValue);
            }
            else if (Types.BOOLEAN.equals(expandedJsonColumn.getColumn().getType())) {
                pageBuilder.setBoolean(expandedJsonColumn.getColumn(), Boolean.parseBoolean(finalValue));
            }
            else if (Types.DOUBLE.equals(expandedJsonColumn.getColumn().getType())) {
                try {
                    pageBuilder.setDouble(expandedJsonColumn.getColumn(), Double.parseDouble(finalValue));
                }
                catch (NumberFormatException e) {
                    throw new JsonValueInvalidException(String.format("Failed to parse '%s' as double", finalValue), e);
                }
            }
            else if (Types.LONG.equals(expandedJsonColumn.getColumn().getType())) {
                try {
                    pageBuilder.setLong(expandedJsonColumn.getColumn(), Long.parseLong(finalValue));
                }
                catch (NumberFormatException e) {
                    // ad-hoc workaround for exponential notation
                    try {
                        pageBuilder.setLong(expandedJsonColumn.getColumn(), (long) Double.parseDouble(finalValue));
                    }
                    catch (NumberFormatException e2) {
                        throw new JsonValueInvalidException(String.format("Failed to parse '%s' as long", finalValue), e);
                    }
                }
            }
            else if (Types.TIMESTAMP.equals(expandedJsonColumn.getColumn().getType())) {
                if (expandedJsonColumn.getTimestampFormatter().isPresent()) {
                    TimestampFormatter formatter = expandedJsonColumn.getTimestampFormatter().get();
                    try {
                        pageBuilder.setTimestamp(expandedJsonColumn.getColumn(), Timestamp.ofInstant(formatter.parse(finalValue)));
                    }
                    catch (DateTimeParseException e) {
                        throw new JsonValueInvalidException(String.format("Failed to parse '%s' as timestamp", finalValue), e);
                    }
                }
                else {
                    throw new RuntimeException("TimestampFormatter is absent for column:" + expandedJsonColumn.getKey());
                }
            }
            else if (Types.JSON.equals(expandedJsonColumn.getColumn().getType())) {
                try {
                    pageBuilder.setJson(expandedJsonColumn.getColumn(), jsonParser.parse(finalValue));
                }
                catch (JsonParseException e) {
                    throw new JsonValueInvalidException(String.format("Failed to parse '%s' as JSON", finalValue), e);
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
