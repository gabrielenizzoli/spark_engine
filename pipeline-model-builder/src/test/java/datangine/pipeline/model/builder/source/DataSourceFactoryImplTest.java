package datangine.pipeline.model.builder.source;

import dataengine.pipeline.core.sink.impl.DataSinkCollectRows;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.model.builder.source.DataSourceFactoryImpl;
import dataengine.pipeline.model.pipeline.step.StepFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
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
        StepFactory steps = Utils.getStepsFactory();
        DataSourceFactory dataSourceFactory = DataSourceFactoryImpl.withStepFactory(steps);

        // when
        DataSinkCollectRows<Row> dataSink = new DataSinkCollectRows<>();
        dataSourceFactory.apply("tx").encodeAsRow().writeTo(dataSink);

        // then
        Assertions.assertEquals(
                Arrays.asList("a:xxx", "b:yyy"),
                dataSink.getRows().stream()
                        .map(r -> r.get(r.fieldIndex("str")) + ":" + r.getString(r.fieldIndex("str2")))
                        .sorted()
                        .collect(Collectors.toList())
        );
    }

}