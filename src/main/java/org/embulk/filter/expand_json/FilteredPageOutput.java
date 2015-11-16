package org.embulk.filter.expand_json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.embulk.filter.expand_json.ExpandJsonFilterPlugin.PluginTask;

/**
 * Created by takahiro.nakayama on 10/19/15.
 */
public class FilteredPageOutput
    implements PageOutput
{
    private final Logger logger = Exec.getLogger(FilteredPageOutput.class);
    private final String jsonPathRoot;
    private final List<Column> inputColumnsExceptExpandedJsonColumn;
    private final List<Column> expandedJsonColumns;
    private final HashMap<String, TimestampParser> timestampParserHashMap;
    private final Column jsonColumn;
    private final PageReader pageReader;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageBuilder pageBuilder;
    private final PageOutput pageOutput;

    FilteredPageOutput(PluginTask task, Schema inputSchema, Schema outputSchema, PageOutput pageOutput)
    {
        this.jsonPathRoot = task.getRoot();

        ImmutableList.Builder<Column> inputColumnsExceptExpandedJsonColumnBuilder = ImmutableList.builder();
        ImmutableList.Builder<Column> expandedJsonColumnsBuilder = ImmutableList.builder();
        for (Column column : outputSchema.getColumns()) {
            if (inputSchema.getColumns().contains(column)) {
                inputColumnsExceptExpandedJsonColumnBuilder.add(column);
            }
            else {
                expandedJsonColumnsBuilder.add(column);
            }
        }
        this.inputColumnsExceptExpandedJsonColumn = inputColumnsExceptExpandedJsonColumnBuilder.build();
        this.expandedJsonColumns = expandedJsonColumnsBuilder.build();

        Column temporaryJsonColumn = null;
        for (Column column: inputSchema.getColumns()) {
            if (column.getName().contentEquals(task.getJsonColumnName())) {
                temporaryJsonColumn = column;
            }
        }
        this.jsonColumn = temporaryJsonColumn;

        this.timestampParserHashMap = buildTimestampParserHashMap(task);
        this.pageReader = new PageReader(inputSchema);
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.pageOutput = pageOutput;
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
    }

    @Override
    public void add(Page page)
    {
        try {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                setInputColumnsExceptExpandedJsonColumns(pageBuilder, inputColumnsExceptExpandedJsonColumn);
                setExpandedJsonColumns(pageBuilder, jsonColumn, expandedJsonColumns, timestampParserHashMap);
                pageBuilder.addRecord();
            }
        }
        catch (JsonProcessingException e) {
            logger.error(e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void finish()
    {
        pageBuilder.finish();
        pageOutput.finish();
    }

    @Override
    public void close()
    {
        pageReader.close();
        pageBuilder.close();
        pageOutput.close();
    }

    private HashMap<String, TimestampParser> buildTimestampParserHashMap(PluginTask task)
    {
        final HashMap<String, TimestampParser> timestampParserHashMap = Maps.newHashMap();
        for (ColumnConfig expandedColumnConfig: task.getExpandedColumns()) {
            if (Types.TIMESTAMP.equals(expandedColumnConfig.getType())) {
                String format;
                if (expandedColumnConfig.getOption().has("format")) {
                    format = expandedColumnConfig.getOption().get(String.class, "format");
                }
                else {
                    format = task.getDefaultTimestampFormat();
                }
                DateTimeZone timezone = DateTimeZone.forID(task.getTimeZone());
                TimestampParser parser = new TimestampParser(task.getJRuby(), format, timezone);

                String columnName = expandedColumnConfig.getName();

                timestampParserHashMap.put(columnName, parser);
            }
        }

        return timestampParserHashMap;
    }
    
    private void setInputColumnsExceptExpandedJsonColumns(PageBuilder pageBuilder, List<Column> inputColumnsExceptExpandedJsonColumn) {
        for (Column inputColumn: inputColumnsExceptExpandedJsonColumn) {
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(inputColumn);
                continue;
            }

            if (Types.STRING.equals(inputColumn.getType())) {
                pageBuilder.setString(inputColumn, pageReader.getString(inputColumn));
            }
            else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                pageBuilder.setBoolean(inputColumn, pageReader.getBoolean(inputColumn));
            }
            else if (Types.DOUBLE.equals(inputColumn.getType())) {
                pageBuilder.setDouble(inputColumn, pageReader.getDouble(inputColumn));
            }
            else if (Types.LONG.equals(inputColumn.getType())) {
                pageBuilder.setLong(inputColumn, pageReader.getLong(inputColumn));
            }
            else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                pageBuilder.setTimestamp(inputColumn, pageReader.getTimestamp(inputColumn));
            }
        }
    }

    private void setExpandedJsonColumns(PageBuilder pageBuilder, Column originalJsonColumn, List<Column> expandedJsonColumns, HashMap<String, TimestampParser> timestampParserMap)
            throws JsonProcessingException
    {
        final ReadContext json;
        if (pageReader.isNull(originalJsonColumn)) {
            json = null;
        }
        else {
            String jsonObject = pageReader.getString(originalJsonColumn);
            Configuration conf = Configuration.defaultConfiguration();
            conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
            conf = conf.addOptions(Option.SUPPRESS_EXCEPTIONS);
            json = JsonPath.using(conf).parse(jsonObject);
        }

        for (Column expandedJsonColumn: expandedJsonColumns) {
            if (json == null) {
                pageBuilder.setNull(expandedJsonColumn);
                continue;
            }

            Object value = json.read(jsonPathRoot + expandedJsonColumn.getName());
            final String finalValue = writeJsonPathValueAsString(value);
            if (finalValue == null) {
                pageBuilder.setNull(expandedJsonColumn);
                continue;
            }

            if (Types.STRING.equals(expandedJsonColumn.getType())) {
                pageBuilder.setString(expandedJsonColumn, finalValue);
            }
            else if (Types.BOOLEAN.equals(expandedJsonColumn.getType())) {
                pageBuilder.setBoolean(expandedJsonColumn, Boolean.parseBoolean(finalValue));
            }
            else if (Types.DOUBLE.equals(expandedJsonColumn.getType())) {
                pageBuilder.setDouble(expandedJsonColumn, Double.parseDouble(finalValue));
            }
            else if (Types.LONG.equals(expandedJsonColumn.getType())) {
                pageBuilder.setLong(expandedJsonColumn, Long.parseLong(finalValue));
            }
            else if (Types.TIMESTAMP.equals(expandedJsonColumn.getType())) {
                TimestampParser parser = timestampParserMap.get(expandedJsonColumn.getName());
                pageBuilder.setTimestamp(expandedJsonColumn, parser.parse(finalValue));
            }
        }
    }

    private String writeJsonPathValueAsString(Object value)
            throws JsonProcessingException
    {
        if (value == null) {
            return null;
        }
        else if (value instanceof List) {
            return new ObjectMapper().writeValueAsString(value);
        }
        else if (value instanceof Map) {
            return new ObjectMapper().writeValueAsString(value);
        }
        else if (value instanceof String) {
            return (String) value;
        }
        else {
            return String.valueOf(value);
        }
    }
    
}
