package sparkengine.plan.app;

import sparkengine.plan.model.builder.ModelFormatException;
import sparkengine.plan.model.builder.PlanResolverException;
import sparkengine.plan.runtime.PipelineRunnersFactoryException;
import sparkengine.plan.runtime.builder.datasetconsumer.GlobalCounterConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;

import java.io.IOException;

class StartTest {

    @Test
    void testMain() throws IOException,
            DatasetConsumerException,
            PlanResolverException,
            PipelineRunnersFactoryException,
            ModelFormatException {

        // given
        System.setProperty("spark.master", "local[1]");
        var args = new String[]{"-p", "./src/test/resources/testPlan.yaml", "-l", "INFO"};

        // when
        Start.main(args);

        // then
        Assertions.assertEquals(1, GlobalCounterConsumer.COUNTER.get("app").get());
    }

}