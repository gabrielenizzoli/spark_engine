package datangine.pipeline.model.builder.source;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.core.sink.impl.DataSinkCollect;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.model.builder.source.DataSourceFactoryImpl;
import dataengine.pipeline.model.pipeline.step.Step;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

class DataSourceFactoryImplTest {

    protected static SparkSession sparkSession;

    @BeforeAll
    static void init() {
        sparkSession = SparkSession.builder().master("local").getOrCreate();
    }

    @AfterAll
    static void close() {
        sparkSession.close();
    }

    @Test
    void testBuilder() throws IOException {

        // given
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File yamlSource = new File("src/test/resources/simplePipeline.yaml");
        Map<String, Step> steps = mapper.readValue(yamlSource, new TypeReference<Map<String, Step>>() {
        });
        DataSourceFactory dataSourceFactory = DataSourceFactoryImpl.builder().steps(steps).build().withCache();

        // when
        DataSinkCollect<Row> dataSink = new DataSinkCollect();
        DataSource dataSource = dataSourceFactory.apply("tx");
        dataSource.toDataFrame().write(dataSink);

        // then
        Assertions.assertEquals(
                Arrays.asList("a:xxx", "b:yyy"),
                dataSink.getList().stream()
                        .map(r -> r.get(r.fieldIndex("str")) + ":" + r.getString(r.fieldIndex("str2")))
                        .sorted()
                        .collect(Collectors.toList()));
    }
}