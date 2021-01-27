package sparkengine.plan.runtime.builder;

import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.plan.runtime.PipelineName;
import sparkengine.plan.runtime.PipelineRunnersFactoryException;
import sparkengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class ModelPipelineRunnersFactoryTest extends SparkSessionBase {

    @Test
    void runWithYamlCatalogs() throws IOException, DatasetConsumerException, PipelineRunnersFactoryException {

        // given
        var planFactory = ModelPipelineRunnersFactory.ofPlan(sparkSession, TestCatalog.getPlan("testPlan"));

        // then
        Assertions.assertEquals(1, planFactory.getPipelineNames().size());
        Assertions.assertEquals(PipelineName.of("sql", "view"), planFactory.getPipelineNames().get(0));

        // when
        planFactory.buildPipelineRunner(planFactory.getPipelineNames().get(0)).run();

        // then
        var list = sparkSession.sql("select * from tmpView").as(Encoders.STRING()).collectAsList();
        Assertions.assertEquals(List.of("value"), list);

    }

}