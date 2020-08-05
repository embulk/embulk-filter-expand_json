package org.embulk.filter.expand_json;

import com.google.common.collect.ImmutableList;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.spi.cache.Cache;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.cache.LRUCache;
import com.jayway.jsonpath.spi.cache.NOOPCache;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class ExpandJsonFilterPlugin
        implements FilterPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(ExpandJsonFilterPlugin.class);

    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("json_column_name")
        String getJsonColumnName();

        @Config("root")
        @ConfigDefault("\"$.\"")
        String getRoot();

        @Config("expanded_columns")
        List<ColumnConfig> getExpandedColumns();

        // default_timezone option from TimestampParser.Task

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

        @Config("keep_expanding_json_column")
        @ConfigDefault("false")
        boolean getKeepExpandingJsonColumn();

        @Config("cache_provider")
        @ConfigDefault("null")
        Optional<String> getCacheProviderName();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        // check if deprecated 'time_zone' option is used.
        if (config.has("time_zone")) {
            throw new ConfigException("'time_zone' option will be deprecated");
        }

        PluginTask task = config.loadConfig(PluginTask.class);

        // set cache provider
        task.getCacheProviderName().ifPresent(this::setCacheProvider);

        // check if a column specified as json_column_name option exists or not
        Column jsonColumn = inputSchema.lookupColumn(task.getJsonColumnName());
        if (jsonColumn.getType() != Types.STRING && jsonColumn.getType() != Types.JSON) {
            // throws ConfigException if the column is not string or json type.
            throw new ConfigException(String.format("A column specified as json_column_name option must be string or json type: %s",
                    new Object[] {jsonColumn.toString()}));
        }
        validateExpandedColumns(task.getExpandedColumns());

        Schema outputSchema = buildOutputSchema(task, inputSchema);
        validateOutputSchema(outputSchema);
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        // set cache provider for mapreduce executor.
        task.getCacheProviderName().ifPresent(this::setCacheProviderOrIgnore);
        return new FilteredPageOutput(task, inputSchema, outputSchema, output);
    }

    private Schema buildOutputSchema(PluginTask task, Schema inputSchema)
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        int i = 0; // columns index
        for (Column inputColumn: inputSchema.getColumns()) {
            if (inputColumn.getName().contentEquals(task.getJsonColumnName())) {
                if (!task.getKeepExpandingJsonColumn()) {
                    logger.info("removed column: name: {}, type: {}, index: {}",
                            inputColumn.getName(),
                            inputColumn.getType(),
                            inputColumn.getIndex());
                }
                else {
                    logger.info("unchanged expanding column: name: {}, type: {}, index: {}",
                            inputColumn.getName(),
                            inputColumn.getType(),
                            i);
                    builder.add(new Column(i++, inputColumn.getName(), inputColumn.getType()));
                }
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

    private void validateExpandedColumns(List<ColumnConfig> expandedColumns)
    {
        List<String> columnList = new ArrayList<>();
        for (ColumnConfig columnConfig: expandedColumns) {
            String columnName = columnConfig.getName();
            if (columnList.contains(columnName)) {
                throw new ConfigException(String.format("Column config for '%s' is duplicated at 'expanded_columns' option", columnName));
            }
            columnList.add(columnName);
        }
    }

    private void validateOutputSchema(Schema outputSchema)
    {
        List<String> columnList = new ArrayList<>();
        for (Column column: outputSchema.getColumns()) {
            String columnName = column.getName();
            if (columnList.contains(columnName)) {
                throw new ConfigException(String.format("Output column '%s' is duplicated. Please check 'expanded_columns' option and Input plugin's settings.", columnName));
            }
            columnList.add(columnName);
        }
    }

    private void setCacheProvider(String cacheProviderName)
    {
        String upperCacheProviderName = cacheProviderName.toUpperCase(Locale.ENGLISH);
        switch (upperCacheProviderName)
        {
            case "LRU":
                CacheProvider.setCache(new LRUCache(400));
                break;

            case "NOOP":
                CacheProvider.setCache(new NOOPCache());
                break;

            default:
                try {
                    Class<?> klass = Class.forName(cacheProviderName);
                    Cache cache = (Cache) klass.newInstance();
                    CacheProvider.setCache(cache);
                }
                catch (ClassNotFoundException | IllegalAccessException | InstantiationException | ClassCastException e) {
                    throw new ConfigException(String.format("Cache Provider '%s' is not supported: %s.", cacheProviderName, e.getMessage()), e);
                }
        }
    }

    private void setCacheProviderOrIgnore(String cacheProviderName)
    {
        try {
            setCacheProvider(cacheProviderName);
        }
        catch (JsonPathException e) {
            logger.debug("Cache:{} is already set.", CacheProvider.getCache().getClass());
        }
    }
}
