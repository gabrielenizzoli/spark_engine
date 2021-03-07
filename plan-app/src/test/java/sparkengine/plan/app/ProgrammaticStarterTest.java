package sparkengine.plan.app;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.app.runner.PlanRunner;
import sparkengine.plan.app.runner.RuntimeArgs;
import sparkengine.plan.runtime.builder.datasetconsumer.GlobalCounterConsumer;
import sparkengine.spark.test.SparkSessionManager;

class ProgrammaticStarterTest extends SparkSessionManager {

    @Test
    void testPlanRunner() throws Throwable {

        // when
        var args = RuntimeArgs.builder().planLocation("./src/test/resources/testPlan.yaml").build();
        PlanRunner.builder()
                .sparkSession(sparkSession)
                .runtimeArgs(args)
                .build()
                .run();

        // then
        Assertions.assertEquals(1, GlobalCounterConsumer.COUNTER.get("app").get());
    }


}