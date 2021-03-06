package sparkengine.plan.runtime.builder.dataset.runner;

import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.runtime.builder.RuntimeContext;
import sparkengine.plan.runtime.builder.TestCatalog;
import sparkengine.plan.runtime.builder.runner.ModelPipelineRunnersFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.plan.runtime.runner.PipelineRunnersFactoryException;
import sparkengine.spark.test.SparkSessionManager;

import java.io.IOException;
import java.util.List;

class ModelPipelineRunnersFactoryTest extends SparkSessionManager {

    @Test
    void runWithYamlCatalogs() throws IOException, DatasetConsumerException, PipelineRunnersFactoryException {

        // given
        var planFactory = ModelPipelineRunnersFactory.ofPlan(TestCatalog.getPlan("testPlan"), RuntimeContext.init(sparkSession));

        // then
        Assertions.assertEquals(1, planFactory.getPipelineNames().size());
        Assertions.assertEquals("pipe1", planFactory.getPipelineNames().iterator().next());

        // when
        planFactory.buildPipelineRunner(planFactory.getPipelineNames().iterator().next()).run();

        // then
        var list = sparkSession.sql("select * from tmpView").as(Encoders.STRING()).collectAsList();
        Assertions.assertEquals(List.of("value"), list);

    }

}