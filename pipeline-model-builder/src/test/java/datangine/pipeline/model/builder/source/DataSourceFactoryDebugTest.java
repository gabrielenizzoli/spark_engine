package datangine.pipeline.model.builder.source;

import dataengine.pipeline.model.builder.source.DataSourceFactoryDebug;
import dataengine.pipeline.model.pipeline.step.StepFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DataSourceFactoryDebugTest {

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
    public void testDebug() {
        DataSourceFactoryDebug factoryDebug = DataSourceFactoryDebug.withStepFactory(Utils.getStepsFactory());
        Dataset<Row> ds = factoryDebug.getOrRun("tx").toDF();
        Assertions.assertNotNull(ds);
    }

}