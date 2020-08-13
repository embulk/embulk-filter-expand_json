package org.embulk.filter.expand_json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.cache.LRUCache;
import com.jayway.jsonpath.spi.cache.NOOPCache;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import static org.embulk.filter.expand_json.ExpandJsonFilterPlugin.Control;
import static org.embulk.filter.expand_json.ExpandJsonFilterPlugin.PluginTask;
import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newBoolean;
import static org.msgpack.value.ValueFactory.newFloat;
import static org.msgpack.value.ValueFactory.newInteger;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newMapBuilder;
import static org.msgpack.value.ValueFactory.newString;

public class TestExpandJsonFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();


    private final String c1Data = "_c1_data";
    // schema object is recreated per test method. Since each test method might require different schema,
    // it's better that this field can be overwritten by each method.
    private Schema schema;
    private ExpandJsonFilterPlugin expandJsonFilterPlugin;

    @Before
    public void createResources()
    {
        schema = schema("_c0", STRING, "_c1", STRING); // default schema
        expandJsonFilterPlugin = new ExpandJsonFilterPlugin();
    }

    @Before
    public void clearCacheProvider()
    {
        // NOTE: CacheProvider has cache as private static variables,
        //       so clear the variables before tests run.
        try {
            Class<?> klass = Class.forName(CacheProvider.class.getName());
            Field cache = klass.getDeclaredField("cache");
            cache.setAccessible(true);
            cache.set(null, null);
            Field cachingEnabled = klass.getDeclaredField("cachingEnabled");
            cachingEnabled.setAccessible(true);
            cachingEnabled.setBoolean(null, false);
        }
        catch (IllegalAccessException | NoSuchFieldException | ClassNotFoundException e) {
            Throwables.propagate(e);
        }
    }

    private ConfigSource getConfigFromYaml(String yaml)
    {
        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yaml);
    }

    private String convertToJsonString(Object object)
    {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getBrokenJsonString()
    {
        return "{\"_j0\": \"te\"\n";
    }

    /*
    Config test
     */

    @Test
    public void testThrowExceptionAbsentJsonColumnName()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "expanded_columns:\n" +
                "  - {name: _c1, type: string}";
        ConfigSource config = getConfigFromYaml(configYaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'json_column_name' is required but not set");
        config.loadConfig(PluginTask.class);
    }

    @Test
    public void testThrowExceptionInvalidJsonColumnName()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: not_exist\n" +
                "expanded_columns:\n" +
                "  - {name: _c1, type: string}";
        ConfigSource config = getConfigFromYaml(configYaml);

        exception.expect(SchemaConfigException.class);
        expandJsonFilterPlugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                // do nothing
            }
        });
    }

    @Test
    public void testThrowExceptionInvalidJsonColumnType()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c2\n" +
                "expanded_columns:\n" +
                "  - {name: _c1, type: string}";
        ConfigSource config = getConfigFromYaml(configYaml);
        schema = schema("_c0", STRING, "_c1", STRING, "_c2", LONG);

        exception.expect(ConfigException.class);
        expandJsonFilterPlugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                // do nothing
            }
        });
    }

    @Test
    public void testThrowExceptionAbsentExpandedColumns()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n";
        ConfigSource config = getConfigFromYaml(configYaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'expanded_columns' is required but not set");
        config.loadConfig(PluginTask.class);
    }

    @Test
    public void testThrowConfigExceptionIfTimeZoneIsUsed()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "time_zone: Asia/Tokyo\n";
        ConfigSource config = getConfigFromYaml(configYaml);
        schema = schema("_c0", STRING, "_c1", STRING);

        exception.expect(ConfigException.class);
        exception.expectMessage("'time_zone' option will be deprecated");
        expandJsonFilterPlugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                // do nothing
            }
        });
    }

    @Test
    public void testThrowExceptionDuplicatedExpandedColumns()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "expanded_columns:\n" +
                "  - {name: _c1, type: string}\n" +
                "  - {name: _c1, type: string}";
        ConfigSource config = getConfigFromYaml(configYaml);
        schema = schema("_c0", STRING, "_c1", STRING);

        exception.expect(ConfigException.class);
        exception.expectMessage("Column config for '_c1' is duplicated at 'expanded_columns' option");
        expandJsonFilterPlugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                // do nothing
            }
        });
    }

    @Test
    public void testThrowExceptionDuplicatedOutputColumns()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "expanded_columns:\n" +
                "  - {name: _c1, type: string}";
        ConfigSource config = getConfigFromYaml(configYaml);
        schema = schema("_c0", STRING, "_c0", STRING, "_c1", STRING);

        exception.expect(ConfigException.class);
        exception.expectMessage("Output column '_c1' is duplicated. Please check 'expanded_columns' option and Input plugin's settings.");
        expandJsonFilterPlugin.transaction(config, schema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                // do nothing
            }
        });
    }

    @Test
    public void testThrowExceptionUnsupportedCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "cache_provider: unsupported_cache_provider\n" +
                "expanded_columns:\n" +
                "  - {name: _e1, type: string}";
        ConfigSource config = getConfigFromYaml(configYaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Cache Provider 'unsupported_cache_provider' is not supported: unsupported_cache_provider.");
        expandJsonFilterPlugin.transaction(config, schema, (taskSource, schema) -> {
            // do nothing
        });
    }

    @Test
    public void testDefaultValue()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "expanded_columns:\n" +
                "  - {name: _j1, type: boolean}\n" +
                "  - {name: _j2, type: long}\n" +
                "  - {name: _j3, type: timestamp}\n" +
                "  - {name: _j4, type: double}\n" +
                "  - {name: _j5, type: string}\n" +
                "  - {name: _j6, type: json}\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        PluginTask task = config.loadConfig(PluginTask.class);

        assertEquals("$.", task.getRoot());
        assertEquals("UTC", task.getDefaultTimeZoneId());
        assertEquals("%Y-%m-%d %H:%M:%S.%N %z", task.getDefaultTimestampFormat());
        assertEquals(false, task.getStopOnInvalidRecord());
        assertEquals(false, task.getKeepExpandingJsonColumn());
        assertEquals(Optional.empty(), task.getCacheProviderName());
        expandJsonFilterPlugin.transaction(config, schema, (taskSource, schema) -> {
            assertEquals(LRUCache.class, CacheProvider.getCache().getClass());
        });
    }

    @Test
    public void testUseNOOPCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: string}";
        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, (taskSource, schema) -> {
            assertEquals(NOOPCache.class, CacheProvider.getCache().getClass());
        });
    }

    @Test
    public void testUseUserDefiledCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "cache_provider: " + MyNOOPCache.class.getName() + "\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: string}";
        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, (taskSource, schema) -> {
            assertEquals(MyNOOPCache.class, CacheProvider.getCache().getClass());
        });
    }

    /*
    Expand Test
     */

    @Test
    public void testUnchangedColumnValues()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c6\n" +
                "root: $.\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: string}\n";
        final ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", STRING, "_c1", BOOLEAN, "_c2", DOUBLE,
                "_c3", LONG, "_c4", TIMESTAMP, "_c5", JSON, "_c6", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            "_v0", // _c0
                            true,  // _c1
                            0.2, // _c2
                            3L,   // _c3
                            Timestamp.ofEpochSecond(4), // _c4
                            newMapBuilder().put(s("_e0"), s("_v5")).build(), // _c5
                            "{\"_e0\":\"_v6\"}")) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                assertEquals(1, records.size());

                Object[] record = records.get(0);
                assertEquals("_v0", record[0]);
                assertEquals(true, record[1]);
                assertEquals(0.2, (double) record[2], 0.0001);
                assertEquals(3L, record[3]);
                assertEquals(Timestamp.ofEpochSecond(4), record[4]);
                assertEquals(newMapBuilder().put(s("_e0"), s("_v5")).build(), record[5]);
            }
        });
    }

    @Test
    public void testStopOnInvalidRecordOption()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: json}\n";
        final ConfigSource conf = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", STRING);

        { // stop_on_invalid_record: false
            ConfigSource config = conf.deepCopy();

            expandJsonFilterPlugin.transaction(config, schema, new Control()
            {
                @Override
                public void run(TaskSource taskSource, Schema outputSchema)
                {
                    MockPageOutput mockPageOutput = new MockPageOutput();

                    try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                        for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                                "{\"_e0\":\"\"}", "{\"_e0\":{}}")) {
                            pageOutput.add(page);
                        }

                        pageOutput.finish();
                    }

                    List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                    assertEquals(1, records.size());
                    assertEquals(0, ((MapValue) records.get(0)[0]).size()); // {}
                }
            });
        }

        { // stop_on_invalid_record: true
            ConfigSource config = conf.deepCopy().set("stop_on_invalid_record", true);

            try {
                expandJsonFilterPlugin.transaction(config, schema, new Control()
                {
                    @Override
                    public void run(TaskSource taskSource, Schema outputSchema)
                    {
                        MockPageOutput mockPageOutput = new MockPageOutput();

                        try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                            for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                                    "{\"_e0\":\"\"}", "{\"_e0\":{}}")) {
                                pageOutput.add(page);
                            }

                            pageOutput.finish();
                        }
                    }
                });
                fail();
            }
            catch (Throwable t) {
                assertEquals(DataException.class, t.getClass());
            }
        }
    }

    @Test
    public void testExpandJsonKeyToSchema()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "expanded_columns:\n" +
                "  - {name: _j1, type: boolean}\n" +
                "  - {name: _j2, type: long}\n" +
                "  - {name: _j3, type: timestamp}\n" +
                "  - {name: _j4, type: double}\n" +
                "  - {name: _j5, type: string}\n" +
                "  - {name: _j6, type: json}\n" +
                "  - {name: _c0, type: string}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                assertEquals(8, outputSchema.getColumnCount());

                Column new_j1 = outputSchema.getColumn(0);
                Column new_j2 = outputSchema.getColumn(1);
                Column new_j3 = outputSchema.getColumn(2);
                Column new_j4 = outputSchema.getColumn(3);
                Column new_j5 = outputSchema.getColumn(4);
                Column new_j6 = outputSchema.getColumn(5);
                Column new_c0 = outputSchema.getColumn(6);
                Column old_c1 = outputSchema.getColumn(7);

                assertEquals("_j1", new_j1.getName());
                assertEquals(BOOLEAN, new_j1.getType());
                assertEquals("_j2", new_j2.getName());
                assertEquals(LONG, new_j2.getType());
                assertEquals("_j3", new_j3.getName());
                assertEquals(TIMESTAMP, new_j3.getType());
                assertEquals("_j4", new_j4.getName());
                assertEquals(DOUBLE, new_j4.getType());
                assertEquals("_j5", new_j5.getName());
                assertEquals(STRING, new_j5.getType());
                assertEquals("_j6", new_j6.getName());
                assertEquals(JSON, new_j6.getType());
                assertEquals("_c0", new_c0.getName());
                assertEquals(STRING, new_c0.getType());
                assertEquals("_c1", old_c1.getName());
                assertEquals(STRING, old_c1.getType());
            }
        });
    }

    @Test
    public void testColumnBasedTimezone()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j1, type: timestamp, format: '%Y-%m-%d %H:%M:%S', timezone: 'Asia/Tokyo'}\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", JSON, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                Value data = newMapBuilder()
                        .put(s("_j0"), s("2014-10-21 04:44:33 +0000"))
                        .put(s("_j1"), s("2014-10-21 04:44:33"))
                        .build();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, data, c1Data)) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals("2014-10-21 04:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(0)).toString());
                    assertEquals("2014-10-20 19:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(1)).toString());
                    assertEquals(c1Data, pageReader.getString(outputSchema.getColumn(2)));
                }
            }
        });
    }

    @Test
    public void testExpandJsonValuesFromJson()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "default_timezone: Asia/Tokyo\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: boolean}\n" +
                "  - {name: _j1, type: long}\n" +
                "  - {name: _j2, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j3, type: double}\n" +
                "  - {name: _j4, type: string}\n" +
                "  - {name: _j5, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j6, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}\n" +
                // JsonPath: https://github.com/jayway/JsonPath
                "  - {name: '_j7.store.book[*].author', type: string}\n" +
                "  - {name: '_j7..book[?(@.price <= $[''_j7''][''expensive''])].author', type: string}\n" +
                "  - {name: '_j7..book[?(@.isbn)]', type: string}\n" +
                "  - {name: '_j7..book[?(@.author =~ /.*REES/i)].title', type: string}\n" +
                "  - {name: '_j7.store.book[2].author', type: string}\n" +
                "  - {name: _c0, type: string}\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", JSON, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                Value data = newMapBuilder()
                        .put(s("_j0"), b(true))
                        .put(s("_j1"), i(2))
                        .put(s("_j2"), s("2014-10-21 04:44:33 +0900"))
                        .put(s("_j3"), f(4.4))
                        .put(s("_j4"), s("v5"))
                        .put(s("_j5"), s("2014-10-21 04:44:33 +0000"))
                        .put(s("_j6"), s("2014-10-21 04:44:33"))
                        .put(s("_j7"), newMapBuilder()
                                .put(s("store"), newMapBuilder()
                                        .put(s("book"), newArray(
                                                newMap(s("author"), s("Nigel Rees"), s("title"), s("Sayings of the Century"), s("price"), f(8.95)),
                                                newMap(s("author"), s("Evelyn Waugh"), s("title"), s("Sword of Honour"), s("price"), f(12.99)),
                                                newMap(s("author"), s("Herman Melville"), s("title"), s("Moby Dick"), s("isbn"), s("0-553-21311-3"), s("price"), f(8.99)),
                                                newMap(s("author"), s("J. R. R. Tolkien"), s("title"), s("The Lord of the Rings"), s("isbn"), s("0-395-19395-8"), s("price"), f(22.99))
                                        ))
                                        .put(s("bicycle"), newMap(s("color"), s("red"), s("price"), f(19.95)))
                                        .build())
                                .put(s("expensive"), i(10))
                                .build())
                        .put(s("_c0"), s("v12"))
                        .build();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, data, c1Data)) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals(true, pageReader.getBoolean(outputSchema.getColumn(0)));
                    assertEquals(2, pageReader.getLong(outputSchema.getColumn(1)));
                    assertEquals("2014-10-20 19:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(2)).toString());
                    assertEquals(String.valueOf(4.4), String.valueOf(pageReader.getDouble(outputSchema.getColumn(3))));
                    assertEquals("v5", pageReader.getString(outputSchema.getColumn(4)));
                    assertEquals("2014-10-21 04:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(5)).toString());
                    assertEquals("2014-10-20 19:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(6)).toString());
                    assertEquals("[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]",
                            pageReader.getString(outputSchema.getColumn(7)));
                    assertEquals("[\"Nigel Rees\",\"Herman Melville\"]", pageReader.getString(outputSchema.getColumn(8)));
                    assertEquals("" +
                                    "[" +
                                    "{\"author\":\"Herman Melville\",\"title\":\"Moby Dick\",\"isbn\":\"0-553-21311-3\",\"price\":8.99}," +
                                    "{\"author\":\"J. R. R. Tolkien\",\"title\":\"The Lord of the Rings\",\"isbn\":\"0-395-19395-8\",\"price\":22.99}" +
                                    "]",
                            pageReader.getString(outputSchema.getColumn(9)));
                    assertEquals("[\"Sayings of the Century\"]", pageReader.getString(outputSchema.getColumn(10)));
                    assertEquals("Herman Melville", pageReader.getString(outputSchema.getColumn(11)));
                    assertEquals("v12", pageReader.getString(outputSchema.getColumn(12)));
                    assertEquals(c1Data, pageReader.getString(outputSchema.getColumn(13)));
                }
            }
        });
    }

    @Test(expected = DataException.class)
    public void testSetExpandedJsonColumnsSetInvalidDoubleValue()
    {
        setExpandedJsonColumnsWithInvalidValue("double", s("abcde"));
    }

    @Test(expected = DataException.class)
    public void testSetExpandedJsonColumnsSetInvalidLongValue()
    {
        setExpandedJsonColumnsWithInvalidValue("long", s("abcde"));
    }

    @Test(expected = DataException.class)
    public void testSetExpandedJsonColumnsSetInvalidTimestampValue()
    {
        setExpandedJsonColumnsWithInvalidValue("timestamp", s("abcde"));
    }

    @Test(expected = DataException.class)
    public void testSetExpandedJsonColumnsSetInvalidJsonValue()
    {
        setExpandedJsonColumnsWithInvalidValue("json", s("abcde"));
    }

    public void setExpandedJsonColumnsWithInvalidValue(String ValidType, final Value invalidValue)
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "stop_on_invalid_record: 1\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "default_timezone: Asia/Tokyo\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: " + ValidType + "}\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", JSON, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                Value data = newMapBuilder()
                        .put(s("_j0"), invalidValue)
                        .build();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, data, c1Data)) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }
            }
        });
    }

    @Test
    public void testExpandedJsonValuesWithKeepJsonColumns()
    {
        final String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c1\n" +
                "root: $.\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: string}\n" +
                "keep_expanding_json_column: true\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", STRING, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            "_v0", "{\"_e0\":\"_ev0\"}")) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                assertEquals(3, outputSchema.getColumnCount());
                Column column;
                { // 1st column
                    column = outputSchema.getColumn(0);
                    assertTrue(column.getName().equals("_c0") && column.getType().equals(STRING));
                }
                { // 2nd column
                    column = outputSchema.getColumn(1);
                    assertTrue(column.getName().equals("_c1") && column.getType().equals(STRING));
                }
                { // 3rd column
                    column = outputSchema.getColumn(2);
                    assertTrue(column.getName().equals("_e0") && column.getType().equals(STRING));
                }

                for (Object[] record : Pages.toObjects(outputSchema, mockPageOutput.pages)) {
                    assertEquals("_v0", record[0]);
                    assertEquals("{\"_e0\":\"_ev0\"}", record[1]);
                    assertEquals("_ev0", record[2]);
                }
            }
        });
    }

    @Test
    public void testExpandSpecialJsonValuesFromString()
    {
        final String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c1\n" +
                "root: $.\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: string}\n" +
                "  - {name: _e1, type: string}\n"; // the value will be null

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", STRING, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            "_v0", "")) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                for (Object[] record : Pages.toObjects(outputSchema, mockPageOutput.pages)) {
                    assertEquals("_v0", record[0]);
                    assertNull(record[1]);
                    assertNull(record[2]);
                }
            }
        });
    }

    private static Value s(String value)
    {
        return newString(value);
    }

    private static Value i(int value)
    {
        return newInteger(value);
    }

    private static Value f(double value)
    {
        return newFloat(value);
    }

    private static Value b(boolean value)
    {
        return newBoolean(value);
    }

    @Test
    public void testExpandJsonValuesFromString()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "default_timezone: Asia/Tokyo\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: boolean}\n" +
                "  - {name: _j1, type: long}\n" +
                "  - {name: _j2, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j3, type: double}\n" +
                "  - {name: _j4, type: string}\n" +
                "  - {name: _j5, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j6, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}\n" +
                // JsonPath: https://github.com/jayway/JsonPath
                "  - {name: '_j7.store.book[*].author', type: string}\n" +
                "  - {name: '_j7..book[?(@.price <= $[''_j7''][''expensive''])].author', type: string}\n" +
                "  - {name: '_j7..book[?(@.isbn)]', type: string}\n" +
                "  - {name: '_j7..book[?(@.author =~ /.*REES/i)].title', type: string}\n" +
                "  - {name: '_j7.store.book[2].author', type: string}\n" +
                "  - {name: _c0, type: string}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource,
                                                                    schema,
                                                                    outputSchema,
                                                                    mockPageOutput);

                ImmutableMap.Builder<String,Object> builder = ImmutableMap.builder();
                builder.put("_j0", true);
                builder.put("_j1", 2);
                builder.put("_j2", "2014-10-21 04:44:33 +0900");
                builder.put("_j3", 4.4);
                builder.put("_j4", "v5");
                builder.put("_j5", "2014-10-21 04:44:33 +0000");
                builder.put("_j6", "2014-10-21 04:44:33");
                builder.put("_j7",
                            ImmutableMap.of("store",
                                            ImmutableMap.of("book",
                                                            ImmutableList.of(ImmutableMap.of("author",
                                                                                             "Nigel Rees",
                                                                                             "title",
                                                                                             "Sayings of the Century",
                                                                                             "price",
                                                                                             8.95),
                                                                             ImmutableMap.of("author",
                                                                                             "Evelyn Waugh",
                                                                                             "title",
                                                                                             "Sword of Honour",
                                                                                             "price",
                                                                                             12.99),
                                                                             ImmutableMap.of("author",
                                                                                             "Herman Melville",
                                                                                             "title",
                                                                                             "Moby Dick",
                                                                                             "isbn",
                                                                                             "0-553-21311-3",
                                                                                             "price",
                                                                                             8.99),
                                                                             ImmutableMap.of("author",
                                                                                             "J. R. R. Tolkien",
                                                                                             "title",
                                                                                             "The Lord of the Rings",
                                                                                             "isbn",
                                                                                             "0-395-19395-8",
                                                                                             "price",
                                                                                             22.99)
                                                            ),
                                                            "bicycle",
                                                            ImmutableMap.of("color",
                                                                            "red",
                                                                            "price",
                                                                            19.95
                                                            )
                                            ),
                                            "expensive",
                                            10
                            )
                            /*
                            {
                                "store": {
                                    "book": [
                                        {
                                            "author": "Nigel Rees",
                                            "title": "Sayings of the Century",
                                            "price": 8.95
                                        },
                                        {
                                            "author": "Evelyn Waugh",
                                            "title": "Sword of Honour",
                                            "price": 12.99
                                        },
                                        {
                                            "author": "Herman Melville",
                                            "title": "Moby Dick",
                                            "isbn": "0-553-21311-3",
                                            "price": 8.99
                                        },
                                        {
                                            "author": "J. R. R. Tolkien",
                                            "title": "The Lord of the Rings",
                                            "isbn": "0-395-19395-8",
                                            "price": 22.99
                                        }
                                    ],
                                    "bicycle": {
                                        "color": "red",
                                        "price": 19.95
                                    }
                                },
                                "expensive": 10
                            }
                             */
                );
                builder.put("_c0", "v12");

                String data = convertToJsonString(builder.build());

                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                                                         schema,
                                                         data, c1Data)) {
                    pageOutput.add(page);
                }

                pageOutput.finish();
                pageOutput.close();

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals(true, pageReader.getBoolean(outputSchema.getColumn(0)));
                    assertEquals(2, pageReader.getLong(outputSchema.getColumn(1)));
                    assertEquals("2014-10-20 19:44:33 UTC",
                                 pageReader.getTimestamp(outputSchema.getColumn(2)).toString());
                    assertEquals(String.valueOf(4.4),
                                 String.valueOf(pageReader.getDouble(outputSchema.getColumn(3))));
                    assertEquals("v5", pageReader.getString(outputSchema.getColumn(4)));
                    assertEquals("2014-10-21 04:44:33 UTC",
                                 pageReader.getTimestamp(outputSchema.getColumn(5)).toString());
                    assertEquals("2014-10-20 19:44:33 UTC",
                                 pageReader.getTimestamp(outputSchema.getColumn(6)).toString());
                    assertEquals("[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]",
                                 pageReader.getString(outputSchema.getColumn(7)));
                    assertEquals("[\"Nigel Rees\",\"Herman Melville\"]",
                                 pageReader.getString(outputSchema.getColumn(8)));
                    assertEquals("" +
                                         "[" +
                                         "{" +
                                         "\"author\":\"Herman Melville\"," +
                                         "\"title\":\"Moby Dick\"," +
                                         "\"isbn\":\"0-553-21311-3\"," +
                                         "\"price\":8.99" +
                                         "}," +
                                         "{" +
                                         "\"author\":\"J. R. R. Tolkien\"," +
                                         "\"title\":\"The Lord of the Rings\"," +
                                         "\"isbn\":\"0-395-19395-8\"," +
                                         "\"price\":22.99" +
                                         "}" +
                                         "]",
                                 pageReader.getString(outputSchema.getColumn(9)));
                    assertEquals("[\"Sayings of the Century\"]",
                                 pageReader.getString(outputSchema.getColumn(10)));
                    assertEquals("Herman Melville",
                                 pageReader.getString(outputSchema.getColumn(11)));
                    assertEquals("v12",
                                 pageReader.getString(outputSchema.getColumn(12)));
                    assertEquals(c1Data,
                                 pageReader.getString(outputSchema.getColumn(13)));
                }
            }
        });
    }

    @Test
    public void testAbortBrokenJsonString()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "default_timezone: Asia/Tokyo\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: string}\n";
        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource,
                                                                    schema,
                                                                    outputSchema,
                                                                    mockPageOutput);

                String data = getBrokenJsonString();
                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                                                         schema,
                                                         data, c1Data)) {
                    exception.expect(InvalidJsonException.class);
                    exception.expectMessage("Unexpected End Of File position 12: null");
                    pageOutput.add(page);
                }

                pageOutput.finish();
                pageOutput.close();

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals("te", pageReader.getString(outputSchema.getColumn(0)));
                }
            }
        });
    }

    @Test
    public void testParseNumbersInExponentialNotation()
    {
        final String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c1\n" +
                "root: $.\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: double}\n" +
                "  - {name: _j1, type: long}\n";
        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                String doubleFloatingPoint = "-1.234e-5";
                double doubleFixedPoint = -0.00001234; // Use in Asserting.
                String longFloatingPoint = "12345e3";
                long longFixedPoint = 12_345_000L; // Use in Asserting.

                String data = String.format(
                        "{\"_j0\":%s, \"_j1\":%s}",
                        doubleFloatingPoint,
                        longFloatingPoint);

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, data, c1Data)) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals(doubleFixedPoint, pageReader.getDouble(outputSchema.getColumn(0)), 0.0);
                    assertEquals(longFixedPoint, pageReader.getLong(outputSchema.getColumn(1)));
                }
            }
        });
    }

    // with NOOPCacheProvider
    // NOTE: The below tests are the same as the above tests except 'cache_provider' setting.

    @Test
    public void testUnchangedColumnValuesWithNOOPCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c6\n" +
                "root: $.\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: string}\n";
        final ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", STRING, "_c1", BOOLEAN, "_c2", DOUBLE,
                "_c3", LONG, "_c4", TIMESTAMP, "_c5", JSON, "_c6", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            "_v0", // _c0
                            true,  // _c1
                            0.2, // _c2
                            3L,   // _c3
                            Timestamp.ofEpochSecond(4), // _c4
                            newMapBuilder().put(s("_e0"), s("_v5")).build(), // _c5
                            "{\"_e0\":\"_v6\"}")) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                assertEquals(1, records.size());

                Object[] record = records.get(0);
                assertEquals("_v0", record[0]);
                assertEquals(true, record[1]);
                assertEquals(0.2, (double) record[2], 0.0001);
                assertEquals(3L, record[3]);
                assertEquals(Timestamp.ofEpochSecond(4), record[4]);
                assertEquals(newMapBuilder().put(s("_e0"), s("_v5")).build(), record[5]);
            }
        });
    }

    @Test
    public void testStopOnInvalidRecordOptionWithNOOPCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: json}\n";
        final ConfigSource conf = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", STRING);

        { // stop_on_invalid_record: false
            ConfigSource config = conf.deepCopy();

            expandJsonFilterPlugin.transaction(config, schema, new Control()
            {
                @Override
                public void run(TaskSource taskSource, Schema outputSchema)
                {
                    MockPageOutput mockPageOutput = new MockPageOutput();

                    try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                        for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                                "{\"_e0\":\"\"}", "{\"_e0\":{}}")) {
                            pageOutput.add(page);
                        }

                        pageOutput.finish();
                    }

                    List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);
                    assertEquals(1, records.size());
                    assertEquals(0, ((MapValue) records.get(0)[0]).size()); // {}
                }
            });
        }

        // NOTE: CacheProvider is set the above test, so need to clear the CacheProvider before the below test.
        clearCacheProvider();
        { // stop_on_invalid_record: true
            ConfigSource config = conf.deepCopy().set("stop_on_invalid_record", true);
            try {
                expandJsonFilterPlugin.transaction(config, schema, new Control()
                {
                    @Override
                    public void run(TaskSource taskSource, Schema outputSchema)
                    {
                        MockPageOutput mockPageOutput = new MockPageOutput();

                        try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                            for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                                    "{\"_e0\":\"\"}", "{\"_e0\":{}}")) {
                                pageOutput.add(page);
                            }

                            pageOutput.finish();
                        }
                    }
                });
                fail();
            }
            catch (Throwable t) {
                t.printStackTrace();
                assertEquals(DataException.class, t.getClass());
            }
        }
    }

    @Test
    public void testExpandJsonKeyToSchemaWithNOOPCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _j1, type: boolean}\n" +
                "  - {name: _j2, type: long}\n" +
                "  - {name: _j3, type: timestamp}\n" +
                "  - {name: _j4, type: double}\n" +
                "  - {name: _j5, type: string}\n" +
                "  - {name: _j6, type: json}\n" +
                "  - {name: _c0, type: string}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                assertEquals(8, outputSchema.getColumnCount());

                Column new_j1 = outputSchema.getColumn(0);
                Column new_j2 = outputSchema.getColumn(1);
                Column new_j3 = outputSchema.getColumn(2);
                Column new_j4 = outputSchema.getColumn(3);
                Column new_j5 = outputSchema.getColumn(4);
                Column new_j6 = outputSchema.getColumn(5);
                Column new_c0 = outputSchema.getColumn(6);
                Column old_c1 = outputSchema.getColumn(7);

                assertEquals("_j1", new_j1.getName());
                assertEquals(BOOLEAN, new_j1.getType());
                assertEquals("_j2", new_j2.getName());
                assertEquals(LONG, new_j2.getType());
                assertEquals("_j3", new_j3.getName());
                assertEquals(TIMESTAMP, new_j3.getType());
                assertEquals("_j4", new_j4.getName());
                assertEquals(DOUBLE, new_j4.getType());
                assertEquals("_j5", new_j5.getName());
                assertEquals(STRING, new_j5.getType());
                assertEquals("_j6", new_j6.getName());
                assertEquals(JSON, new_j6.getType());
                assertEquals("_c0", new_c0.getName());
                assertEquals(STRING, new_c0.getType());
                assertEquals("_c1", old_c1.getName());
                assertEquals(STRING, old_c1.getType());
            }
        });
    }

    @Test
    public void testColumnBasedTimezoneWithNOOPCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j1, type: timestamp, format: '%Y-%m-%d %H:%M:%S', timezone: 'Asia/Tokyo'}\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", JSON, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                Value data = newMapBuilder()
                        .put(s("_j0"), s("2014-10-21 04:44:33 +0000"))
                        .put(s("_j1"), s("2014-10-21 04:44:33"))
                        .build();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, data, c1Data)) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals("2014-10-21 04:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(0)).toString());
                    assertEquals("2014-10-20 19:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(1)).toString());
                    assertEquals(c1Data, pageReader.getString(outputSchema.getColumn(2)));
                }
            }
        });
    }

    @Test
    public void testExpandJsonValuesFromJsonWithNOOPCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "default_timezone: Asia/Tokyo\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: boolean}\n" +
                "  - {name: _j1, type: long}\n" +
                "  - {name: _j2, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j3, type: double}\n" +
                "  - {name: _j4, type: string}\n" +
                "  - {name: _j5, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j6, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}\n" +
                // JsonPath: https://github.com/jayway/JsonPath
                "  - {name: '_j7.store.book[*].author', type: string}\n" +
                "  - {name: '_j7..book[?(@.price <= $[''_j7''][''expensive''])].author', type: string}\n" +
                "  - {name: '_j7..book[?(@.isbn)]', type: string}\n" +
                "  - {name: '_j7..book[?(@.author =~ /.*REES/i)].title', type: string}\n" +
                "  - {name: '_j7.store.book[2].author', type: string}\n" +
                "  - {name: _c0, type: string}\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", JSON, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                Value data = newMapBuilder()
                        .put(s("_j0"), b(true))
                        .put(s("_j1"), i(2))
                        .put(s("_j2"), s("2014-10-21 04:44:33 +0900"))
                        .put(s("_j3"), f(4.4))
                        .put(s("_j4"), s("v5"))
                        .put(s("_j5"), s("2014-10-21 04:44:33 +0000"))
                        .put(s("_j6"), s("2014-10-21 04:44:33"))
                        .put(s("_j7"), newMapBuilder()
                                .put(s("store"), newMapBuilder()
                                        .put(s("book"), newArray(
                                                newMap(s("author"), s("Nigel Rees"), s("title"), s("Sayings of the Century"), s("price"), f(8.95)),
                                                newMap(s("author"), s("Evelyn Waugh"), s("title"), s("Sword of Honour"), s("price"), f(12.99)),
                                                newMap(s("author"), s("Herman Melville"), s("title"), s("Moby Dick"), s("isbn"), s("0-553-21311-3"), s("price"), f(8.99)),
                                                newMap(s("author"), s("J. R. R. Tolkien"), s("title"), s("The Lord of the Rings"), s("isbn"), s("0-395-19395-8"), s("price"), f(22.99))
                                        ))
                                        .put(s("bicycle"), newMap(s("color"), s("red"), s("price"), f(19.95)))
                                        .build())
                                .put(s("expensive"), i(10))
                                .build())
                        .put(s("_c0"), s("v12"))
                        .build();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, data, c1Data)) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals(true, pageReader.getBoolean(outputSchema.getColumn(0)));
                    assertEquals(2, pageReader.getLong(outputSchema.getColumn(1)));
                    assertEquals("2014-10-20 19:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(2)).toString());
                    assertEquals(String.valueOf(4.4), String.valueOf(pageReader.getDouble(outputSchema.getColumn(3))));
                    assertEquals("v5", pageReader.getString(outputSchema.getColumn(4)));
                    assertEquals("2014-10-21 04:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(5)).toString());
                    assertEquals("2014-10-20 19:44:33 UTC", pageReader.getTimestamp(outputSchema.getColumn(6)).toString());
                    assertEquals("[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]",
                            pageReader.getString(outputSchema.getColumn(7)));
                    assertEquals("[\"Nigel Rees\",\"Herman Melville\"]", pageReader.getString(outputSchema.getColumn(8)));
                    assertEquals("" +
                                    "[" +
                                    "{\"author\":\"Herman Melville\",\"title\":\"Moby Dick\",\"isbn\":\"0-553-21311-3\",\"price\":8.99}," +
                                    "{\"author\":\"J. R. R. Tolkien\",\"title\":\"The Lord of the Rings\",\"isbn\":\"0-395-19395-8\",\"price\":22.99}" +
                                    "]",
                            pageReader.getString(outputSchema.getColumn(9)));
                    assertEquals("[\"Sayings of the Century\"]", pageReader.getString(outputSchema.getColumn(10)));
                    assertEquals("Herman Melville", pageReader.getString(outputSchema.getColumn(11)));
                    assertEquals("v12", pageReader.getString(outputSchema.getColumn(12)));
                    assertEquals(c1Data, pageReader.getString(outputSchema.getColumn(13)));
                }
            }
        });
    }

    @Test(expected = DataException.class)
    public void testSetExpandedJsonColumnsSetInvalidDoubleValueWithNOOPCacheProvider()
    {
        setExpandedJsonColumnsWithInvalidValueWithNOOPCacheProvider("double", s("abcde"));
    }

    @Test(expected = DataException.class)
    public void testSetExpandedJsonColumnsSetInvalidLongValueWithNOOPCacheProvider()
    {
        setExpandedJsonColumnsWithInvalidValueWithNOOPCacheProvider("long", s("abcde"));
    }

    @Test(expected = DataException.class)
    public void testSetExpandedJsonColumnsSetInvalidTimestampValueWithNOOPCacheProvider()
    {
        setExpandedJsonColumnsWithInvalidValueWithNOOPCacheProvider("timestamp", s("abcde"));
    }

    @Test(expected = DataException.class)
    public void testSetExpandedJsonColumnsSetInvalidJsonValueWithNOOPCacheProvider()
    {
        setExpandedJsonColumnsWithInvalidValueWithNOOPCacheProvider("json", s("abcde"));
    }

    public void setExpandedJsonColumnsWithInvalidValueWithNOOPCacheProvider(String ValidType, final Value invalidValue)
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "stop_on_invalid_record: 1\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "default_timezone: Asia/Tokyo\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: " + ValidType + "}\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", JSON, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                Value data = newMapBuilder()
                        .put(s("_j0"), invalidValue)
                        .build();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, data, c1Data)) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }
            }
        });
    }

    @Test
    public void testExpandedJsonValuesWithKeepJsonColumnsWithNOOPCacheProvider()
    {
        final String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c1\n" +
                "root: $.\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: string}\n" +
                "keep_expanding_json_column: true\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", STRING, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            "_v0", "{\"_e0\":\"_ev0\"}")) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                assertEquals(3, outputSchema.getColumnCount());
                Column column;
                { // 1st column
                    column = outputSchema.getColumn(0);
                    assertTrue(column.getName().equals("_c0") && column.getType().equals(STRING));
                }
                { // 2nd column
                    column = outputSchema.getColumn(1);
                    assertTrue(column.getName().equals("_c1") && column.getType().equals(STRING));
                }
                { // 3rd column
                    column = outputSchema.getColumn(2);
                    assertTrue(column.getName().equals("_e0") && column.getType().equals(STRING));
                }

                for (Object[] record : Pages.toObjects(outputSchema, mockPageOutput.pages)) {
                    assertEquals("_v0", record[0]);
                    assertEquals("{\"_e0\":\"_ev0\"}", record[1]);
                    assertEquals("_ev0", record[2]);
                }
            }
        });
    }

    @Test
    public void testExpandSpecialJsonValuesFromStringWithNOOPCacheProvider()
    {
        final String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c1\n" +
                "root: $.\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _e0, type: string}\n" +
                "  - {name: _e1, type: string}\n"; // the value will be null

        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c0", STRING, "_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                            "_v0", "")) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                for (Object[] record : Pages.toObjects(outputSchema, mockPageOutput.pages)) {
                    assertEquals("_v0", record[0]);
                    assertNull(record[1]);
                    assertNull(record[2]);
                }
            }
        });
    }

    @Test
    public void testExpandJsonValuesFromStringWithNOOPCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "default_timezone: Asia/Tokyo\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: boolean}\n" +
                "  - {name: _j1, type: long}\n" +
                "  - {name: _j2, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j3, type: double}\n" +
                "  - {name: _j4, type: string}\n" +
                "  - {name: _j5, type: timestamp, format: '%Y-%m-%d %H:%M:%S %z'}\n" +
                "  - {name: _j6, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}\n" +
                // JsonPath: https://github.com/jayway/JsonPath
                "  - {name: '_j7.store.book[*].author', type: string}\n" +
                "  - {name: '_j7..book[?(@.price <= $[''_j7''][''expensive''])].author', type: string}\n" +
                "  - {name: '_j7..book[?(@.isbn)]', type: string}\n" +
                "  - {name: '_j7..book[?(@.author =~ /.*REES/i)].title', type: string}\n" +
                "  - {name: '_j7.store.book[2].author', type: string}\n" +
                "  - {name: _c0, type: string}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource,
                        schema,
                        outputSchema,
                        mockPageOutput);

                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                builder.put("_j0", true);
                builder.put("_j1", 2);
                builder.put("_j2", "2014-10-21 04:44:33 +0900");
                builder.put("_j3", 4.4);
                builder.put("_j4", "v5");
                builder.put("_j5", "2014-10-21 04:44:33 +0000");
                builder.put("_j6", "2014-10-21 04:44:33");
                builder.put("_j7",
                        ImmutableMap.of("store",
                                ImmutableMap.of("book",
                                        ImmutableList.of(ImmutableMap.of("author",
                                                "Nigel Rees",
                                                "title",
                                                "Sayings of the Century",
                                                "price",
                                                8.95),
                                                ImmutableMap.of("author",
                                                        "Evelyn Waugh",
                                                        "title",
                                                        "Sword of Honour",
                                                        "price",
                                                        12.99),
                                                ImmutableMap.of("author",
                                                        "Herman Melville",
                                                        "title",
                                                        "Moby Dick",
                                                        "isbn",
                                                        "0-553-21311-3",
                                                        "price",
                                                        8.99),
                                                ImmutableMap.of("author",
                                                        "J. R. R. Tolkien",
                                                        "title",
                                                        "The Lord of the Rings",
                                                        "isbn",
                                                        "0-395-19395-8",
                                                        "price",
                                                        22.99)
                                        ),
                                        "bicycle",
                                        ImmutableMap.of("color",
                                                "red",
                                                "price",
                                                19.95
                                        )
                                ),
                                "expensive",
                                10
                        )
                            /*
                            {
                                "store": {
                                    "book": [
                                        {
                                            "author": "Nigel Rees",
                                            "title": "Sayings of the Century",
                                            "price": 8.95
                                        },
                                        {
                                            "author": "Evelyn Waugh",
                                            "title": "Sword of Honour",
                                            "price": 12.99
                                        },
                                        {
                                            "author": "Herman Melville",
                                            "title": "Moby Dick",
                                            "isbn": "0-553-21311-3",
                                            "price": 8.99
                                        },
                                        {
                                            "author": "J. R. R. Tolkien",
                                            "title": "The Lord of the Rings",
                                            "isbn": "0-395-19395-8",
                                            "price": 22.99
                                        }
                                    ],
                                    "bicycle": {
                                        "color": "red",
                                        "price": 19.95
                                    }
                                },
                                "expensive": 10
                            }
                             */
                );
                builder.put("_c0", "v12");

                String data = convertToJsonString(builder.build());

                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                        schema,
                        data, c1Data)) {
                    pageOutput.add(page);
                }

                pageOutput.finish();
                pageOutput.close();

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals(true, pageReader.getBoolean(outputSchema.getColumn(0)));
                    assertEquals(2, pageReader.getLong(outputSchema.getColumn(1)));
                    assertEquals("2014-10-20 19:44:33 UTC",
                            pageReader.getTimestamp(outputSchema.getColumn(2)).toString());
                    assertEquals(String.valueOf(4.4),
                            String.valueOf(pageReader.getDouble(outputSchema.getColumn(3))));
                    assertEquals("v5", pageReader.getString(outputSchema.getColumn(4)));
                    assertEquals("2014-10-21 04:44:33 UTC",
                            pageReader.getTimestamp(outputSchema.getColumn(5)).toString());
                    assertEquals("2014-10-20 19:44:33 UTC",
                            pageReader.getTimestamp(outputSchema.getColumn(6)).toString());
                    assertEquals("[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]",
                            pageReader.getString(outputSchema.getColumn(7)));
                    assertEquals("[\"Nigel Rees\",\"Herman Melville\"]",
                            pageReader.getString(outputSchema.getColumn(8)));
                    assertEquals("" +
                                    "[" +
                                    "{" +
                                    "\"author\":\"Herman Melville\"," +
                                    "\"title\":\"Moby Dick\"," +
                                    "\"isbn\":\"0-553-21311-3\"," +
                                    "\"price\":8.99" +
                                    "}," +
                                    "{" +
                                    "\"author\":\"J. R. R. Tolkien\"," +
                                    "\"title\":\"The Lord of the Rings\"," +
                                    "\"isbn\":\"0-395-19395-8\"," +
                                    "\"price\":22.99" +
                                    "}" +
                                    "]",
                            pageReader.getString(outputSchema.getColumn(9)));
                    assertEquals("[\"Sayings of the Century\"]",
                            pageReader.getString(outputSchema.getColumn(10)));
                    assertEquals("Herman Melville",
                            pageReader.getString(outputSchema.getColumn(11)));
                    assertEquals("v12",
                            pageReader.getString(outputSchema.getColumn(12)));
                    assertEquals(c1Data,
                            pageReader.getString(outputSchema.getColumn(13)));
                }
            }
        });
    }

    @Test
    public void testAbortBrokenJsonStringWithNOOPCacheProvider()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "default_timezone: Asia/Tokyo\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: string}\n";
        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();
                PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource,
                        schema,
                        outputSchema,
                        mockPageOutput);

                String data = getBrokenJsonString();
                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                        schema,
                        data, c1Data)) {
                    exception.expect(InvalidJsonException.class);
                    exception.expectMessage("Unexpected End Of File position 12: null");
                    pageOutput.add(page);
                }

                pageOutput.finish();
                pageOutput.close();

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals("te", pageReader.getString(outputSchema.getColumn(0)));
                }
            }
        });
    }

    @Test
    public void testParseNumbersInExponentialNotationWithNOOPCacheProvider()
    {
        final String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c1\n" +
                "root: $.\n" +
                "cache_provider: noop\n" +
                "expanded_columns:\n" +
                "  - {name: _j0, type: double}\n" +
                "  - {name: _j1, type: long}\n";
        ConfigSource config = getConfigFromYaml(configYaml);
        final Schema schema = schema("_c1", STRING);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                String doubleFloatingPoint = "-1.234e-5";
                double doubleFixedPoint = -0.00001234; // Use in Asserting.
                String longFloatingPoint = "12345e3";
                long longFixedPoint = 12_345_000L; // Use in Asserting.

                String data = String.format(
                        "{\"_j0\":%s, \"_j1\":%s}",
                        doubleFloatingPoint,
                        longFloatingPoint);

                try (PageOutput pageOutput = expandJsonFilterPlugin.open(taskSource, schema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, data, c1Data)) {
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                }

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals(doubleFixedPoint, pageReader.getDouble(outputSchema.getColumn(0)), 0.0);
                    assertEquals(longFixedPoint, pageReader.getLong(outputSchema.getColumn(1)));
                }
            }
        });
    }


    private static Schema schema(Object... nameAndTypes)
    {
        Schema.Builder builder = Schema.builder();
        for (int i = 0; i < nameAndTypes.length; i += 2) {
            builder.add((String) nameAndTypes[i], (Type) nameAndTypes[i + 1]);
        }
        return builder.build();
    }
}
