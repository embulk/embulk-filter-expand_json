package org.embulk.filter.expand_json;

import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

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

        @Config("root")
        @ConfigDefault("\"$.\"")
        public String getRoot();

        @Config("expanded_columns")
        public List<ColumnConfig> getExpandedColumns();

        // Time zone of timestamp columns if the value itself doesn’t include time zone description (eg. Asia/Tokyo)
        @Config("time_zone")
        @ConfigDefault("\"UTC\"")
        public String getTimeZone();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // check if a column specified as json_column_name option exists or not
        Column jsonColumn = inputSchema.lookupColumn(task.getJsonColumnName());
        if (jsonColumn.getType() != Types.STRING && jsonColumn.getType() != Types.JSON) {
            // throws ConfigException if the column is not string or json type.
            throw new ConfigException(String.format("A column specified as json_column_name option must be string or json type: %s",
                    new Object[] {jsonColumn.toString()}));
        }

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
                logger.info("removed column: name: {}, type: {}, index: {}",
                            inputColumn.getName(),
                            inputColumn.getType(),
                            inputColumn.getIndex());
                for (ColumnConfig expandedColumnConfig: task.getExpandedColumns()) {
                    logger.info("added column: name: {}, type: {}, options: {}, index: {}",
                                expandedColumnConfig.getName(),
                                expandedColumnConfig.getType(),
                                expandedColumnConfig.getOption(),
                                i);
                    Column outputColumn = new Column(i++,
                                                     expandedColumnConfig.getName(),
                                                     expandedColumnConfig.getType());
                    builder.add(outputColumn);
                }
            }
            else {
                logger.info("unchanged column: name: {}, type: {}, index: {}",
                            inputColumn.getName(),
                            inputColumn.getType(),
                            i);
                Column outputColumn = new Column(i++,
                                                 inputColumn.getName(),
                                                 inputColumn.getType());
                builder.add(outputColumn);
            }
        }

        return new Schema(builder.build());
    }

}
