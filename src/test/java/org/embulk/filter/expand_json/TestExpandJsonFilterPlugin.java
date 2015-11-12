package org.embulk.filter.expand_json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.InvalidJsonException;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.embulk.filter.expand_json.ExpandJsonFilterPlugin.Control;
import static org.embulk.filter.expand_json.ExpandJsonFilterPlugin.PluginTask;
import static org.embulk.spi.type.Types.*;
import static org.junit.Assert.assertEquals;

public class TestExpandJsonFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();


    private final Schema schema = Schema.builder()
            .add("_c0", STRING)
            .build();
    private ExpandJsonFilterPlugin expandJsonFilterPlugin;

    @Before
    public void createResources()
    {
        expandJsonFilterPlugin = new ExpandJsonFilterPlugin();
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
                "  - {name: _j5, type: string}\n";

        ConfigSource config = getConfigFromYaml(configYaml);
        PluginTask task = config.loadConfig(PluginTask.class);

        assertEquals("$.", task.getRoot());
        assertEquals("UTC", task.getTimeZone());
        assertEquals("%Y-%m-%d %H:%M:%S.%N %z", task.getDefaultTimestampFormat());
    }

    /*
    Expand Test
     */

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
                "  - {name: _j5, type: string}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        expandJsonFilterPlugin.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                assertEquals(5, outputSchema.getColumnCount());

                Column new_j1 = outputSchema.getColumn(0);
                Column new_j2 = outputSchema.getColumn(1);
                Column new_j3 = outputSchema.getColumn(2);
                Column new_j4 = outputSchema.getColumn(3);
                Column new_j5 = outputSchema.getColumn(4);

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
            }
        });
    }

    @Test
    public void testExpandJsonValues()
    {
        String configYaml = "" +
                "type: expand_json\n" +
                "json_column_name: _c0\n" +
                "root: $.\n" +
                "time_zone: Asia/Tokyo\n" +
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
                "  - {name: '_j7.store.book[2].author', type: string}\n";

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

                String data = convertToJsonString(builder.build());

                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                                                         schema,
                                                         data)) {
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
                    assertEquals("[" +
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
                "time_zone: Asia/Tokyo\n" +
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
                                                         data)) {
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

}
