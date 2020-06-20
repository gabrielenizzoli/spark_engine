package datangine.pipeline_builder.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.model.pipeline.Pipeline;
import dataengine.model.pipeline.sink.Sink;
import dataengine.pipeline.DataSource;
import datangine.pipeline_builder.sink.DataSinkFactory;
import datangine.pipeline_builder.sink.DataSinkFactoryImpl;
import datangine.pipeline_builder.source.DataSourceFactory;
import datangine.pipeline_builder.source.DataSourceFactoryImpl;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

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
        Pipeline pipeline = mapper.readValue(yamlSource, Pipeline.class);
        DataSourceFactory dataSourceFactory = DataSourceFactoryImpl.builder().steps(pipeline.getSteps()).build().withCache();
        DataSinkFactory dataSinkFactory = DataSinkFactoryImpl.builder().build();

        // when
        Sink sink = pipeline.getSinks().get("show");
        DataSource dataSource = dataSourceFactory.apply(sink.getUsing());
        dataSource.write(dataSinkFactory.apply(sink));

        // then
        // ...
    }
}