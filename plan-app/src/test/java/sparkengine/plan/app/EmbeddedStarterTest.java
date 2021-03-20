package sparkengine.plan.app;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.app.runner.PlanRunner;
import sparkengine.plan.app.runner.RuntimeArgs;
import sparkengine.plan.runtime.builder.datasetconsumer.GlobalCounterConsumer;
import sparkengine.spark.test.SparkSessionManager;

import java.util.Optional;

class EmbeddedStarterTest extends SparkSessionManager {

    @Test
    void testPlanRunner() throws Throwable {

        // given
        Optional.ofNullable(GlobalCounterConsumer.COUNTER.get("app")).ifPresent(c -> c.set(0));

        // when
        var args = RuntimeArgs.builder().build();
        PlanRunner.builder()
                .sparkSession(sparkSession)
                .planLocation("./src/test/resources/testPlan.yaml")
                .runtimeArgs(args)
                .build()
                .run();

        // then
        Assertions.assertEquals(1, GlobalCounterConsumer.COUNTER.get("app").get());
    }


}