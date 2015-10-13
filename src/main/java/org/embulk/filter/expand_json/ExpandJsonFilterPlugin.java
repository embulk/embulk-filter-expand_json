package org.embulk.filter.expand_json;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ExpandJsonFilterPlugin
        implements FilterPlugin
{
    private final Logger logger = Exec.getLogger(ExpandJsonFilterPlugin.class);

    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("json_column_name")
        public String getJsonColumnName();

        @Config("expanded_columns")
        public List<ColumnConfig> getExpandedColumns();

        @Config("time_zone")
        @ConfigDefault("\"UTC\"")
        public String getTimeZone();

//        TODO if needed: add the original column name as the prefix of expanded
//        @Config("add_original_column_name_as_prefix")
//        @ConfigDefault("false")
//        public String getOption2();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = buildOutputSchema(inputSchema,
                                                task.getJsonColumnName(),
                                                task.getExpandedColumns());

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        final List<Column> inputColumns = inputSchema.getColumns();

        final List<Column> inputColumnsExceptExpandedJsonColumn = new ArrayList<>();
        final List<Column> expandedJsonColumns = new ArrayList<>();

        for (Column column : outputSchema.getColumns()) {
            if (inputColumns.contains(column)) {
                inputColumnsExceptExpandedJsonColumn.add(column);
            }
            else {
                expandedJsonColumns.add(column);
            }
        }

        Column temporaryJsonColumn = null;
        for (Column column: inputColumns) {
            if (column.getName().contentEquals(task.getJsonColumnName())) {
                temporaryJsonColumn = column;
            }
        }
        final Column jsonColumn = temporaryJsonColumn;

        final HashMap<String, TimestampParser> timestampParserMap = buildTimestampParserMap(task.getJRuby(),
                                                                                            task.getExpandedColumns(),
                                                                                            task.getTimeZone());
        return new PageOutput()
        {
            private PageReader pageReader = new PageReader(inputSchema);

            @Override
            public void add(Page page)
            {
                try (PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output)) {
                    pageReader.setPage(page);

                    while (pageReader.nextRecord()) {
                        setInputColumnsExceptFlattenJsonColumns(pageBuilder, inputColumnsExceptExpandedJsonColumn);
                        setExpandedJsonColumns(pageBuilder, jsonColumn, expandedJsonColumns, timestampParserMap);
                        pageBuilder.addRecord();
                    }
                    pageBuilder.finish();
                }
            }

            @Override
            public void finish()
            {
                output.finish();
            }

            @Override
            public void close()
            {
                pageReader.close();
                output.close();
            }

            private void setInputColumnsExceptFlattenJsonColumns(PageBuilder pageBuilder, List<Column> inputColumnsExceptExpandedJsonColumn) {
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
            {
                final ReadContext json;
                if (pageReader.isNull(originalJsonColumn)) {
                    json = null;
                }
                else {
                    String jsonObject = pageReader.getString(originalJsonColumn);
                    json = JsonPath.parse(jsonObject);
                }

                for (Column expandedJsonColumn: expandedJsonColumns) {
                    if (json == null) {
                        pageBuilder.setNull(expandedJsonColumn);
                        continue;
                    }

                    try {
                        if (Types.STRING.equals(expandedJsonColumn.getType())) {
                            pageBuilder.setString(expandedJsonColumn, json.read(expandedJsonColumn.getName(), String.class));
                        }
                        else if (Types.BOOLEAN.equals(expandedJsonColumn.getType())) {
                            pageBuilder.setBoolean(expandedJsonColumn, json.read(expandedJsonColumn.getName(), Boolean.class));
                        }
                        else if (Types.DOUBLE.equals(expandedJsonColumn.getType())) {
                            pageBuilder.setDouble(expandedJsonColumn, json.read(expandedJsonColumn.getName(), Double.class));
                        }
                        else if (Types.LONG.equals(expandedJsonColumn.getType())) {
                            pageBuilder.setLong(expandedJsonColumn, json.read(expandedJsonColumn.getName(), Long.class));
                        }
                        else if (Types.TIMESTAMP.equals(expandedJsonColumn.getType())) {
                            TimestampParser parser = timestampParserMap.get(expandedJsonColumn.getName());
                            pageBuilder.setTimestamp(expandedJsonColumn, parser.parse(json.read(expandedJsonColumn.getName(), String.class)));
                        }
                    }
                    catch (PathNotFoundException e) {
                        pageBuilder.setNull(expandedJsonColumn);
                        continue;
                    }
                }
            }

        };
    }

    private Schema buildOutputSchema(Schema inputSchema, String jsonColumnName, List<ColumnConfig> expandedColumnConfigs)
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        int i = 0; // columns index
        for (Column inputColumn: inputSchema.getColumns()) {
            if (inputColumn.getName().contentEquals(jsonColumnName)) {
                logger.info("removed column: name: {}, type: {}",
                            inputColumn.getName(),
                            inputColumn.getType());
                for (ColumnConfig expandedColumnConfig: expandedColumnConfigs) {
                    logger.info("added column: name: {}, type: {}, options: {}",
                                expandedColumnConfig.getName(),
                                expandedColumnConfig.getType(),
                                expandedColumnConfig.getOption());
                    Column outputColumn = new Column(i++,
                                                     expandedColumnConfig.getName(),
                                                     expandedColumnConfig.getType());
                    builder.add(outputColumn);
                }
            }
            else {
                Column outputColumn = new Column(i++,
                                                 inputColumn.getName(),
                                                 inputColumn.getType());
                builder.add(outputColumn);
            }
        }

        return new Schema(builder.build());
    }

    private HashMap<String, TimestampParser> buildTimestampParserMap(ScriptingContainer jruby, List<ColumnConfig> expandedColumnConfigs, String timeZone)
    {
        final HashMap<String, TimestampParser> timestampParserMap = Maps.newHashMap();
        for (ColumnConfig expandedColumnConfig: expandedColumnConfigs) {
            if (Types.TIMESTAMP.equals(expandedColumnConfig.getType())) {
                String format = expandedColumnConfig.getOption().get(String.class, "format");
                DateTimeZone timezone = DateTimeZone.forID(timeZone);
                TimestampParser parser = new TimestampParser(jruby, format, timezone);

                String columnName = expandedColumnConfig.getName();

                timestampParserMap.put(columnName, parser);
            }
        }

        return timestampParserMap;
    }
}
