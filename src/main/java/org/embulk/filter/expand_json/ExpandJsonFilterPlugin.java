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
import java.util.Map;

public class ExpandJsonFilterPlugin
        implements FilterPlugin
{
    private final Logger logger = Exec.getLogger(ExpandJsonFilterPlugin.class);

    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("json_column_name")
        public String getJsonColumnName();

        @Config("root")
        @ConfigDefault("\"$.\"")
        public String getRoot();

        @Config("expanded_columns")
        public List<ColumnConfig> getExpandedColumns();

        @Config("time_zone")
        @ConfigDefault("\"UTC\"")
        public String getTimeZone();

    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema outputSchema = buildOutputSchema(task, inputSchema);
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        return new FilteredPageOutput(task, inputSchema, outputSchema, output);
    }

    private Schema buildOutputSchema(PluginTask task, Schema inputSchema)
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        int i = 0; // columns index
        for (Column inputColumn: inputSchema.getColumns()) {
            if (inputColumn.getName().contentEquals(task.getJsonColumnName())) {
                logger.info("removed column: name: {}, type: {}",
                            inputColumn.getName(),
                            inputColumn.getType());
                for (ColumnConfig expandedColumnConfig: task.getExpandedColumns()) {
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

}
