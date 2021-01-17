package dataengine.pipeline.runtime.builder.plan;

import dataengine.pipeline.runtime.builder.TestCatalog;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.pipeline.runtime.plan.PipelineName;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class ModelPlanFactoryTest extends SparkSessionBase {

    @Test
    void runWithYamlCatalogs() throws IOException, DatasetFactoryException, DatasetConsumerFactoryException, DatasetConsumerException {

        // given
        var planFactory = ModelPlanFactory.ofPlan(sparkSession, TestCatalog.getPlan("testPlan"));

        // then
        Assertions.assertEquals(1, planFactory.getPipelineNames().size());
        Assertions.assertEquals(PipelineName.of("sql", "view"), planFactory.getPipelineNames().get(0));

        // when
        planFactory.getAllRunners().get(0).run();

        // then
        var list = sparkSession.sql("select * from tmpView").as(Encoders.STRING()).collectAsList();
        Assertions.assertEquals(List.of("value"), list);

    }

}