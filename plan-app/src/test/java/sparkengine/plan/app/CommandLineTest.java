package sparkengine.plan.app;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.runtime.builder.datasetconsumer.GlobalCounterConsumer;

import java.util.Optional;

class CommandLineTest {

    @Test
    void testMain() throws Throwable {

        // given
        System.setProperty("spark.master", "local[1]");
        Optional.ofNullable(GlobalCounterConsumer.COUNTER.get("app")).ifPresent(c -> c.set(0));
        var args = new String[]{"-p", "./src/test/resources/testPlan.yaml", "-l", "INFO"};

        // when
        Start.main(args);

        // then
        Assertions.assertEquals(1, GlobalCounterConsumer.COUNTER.get("app").get());
    }

    @Test
    void testMain2() throws Throwable {

        // given
        System.setProperty("spark.master", "local");
        //System.setProperty("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink");
        var args = new String[]{"-p", "/home/gabe/code/github/spark_engine/examples/plans/quickStartPlan.yaml", "-l", "INFO"};

        // when
        Start.main(args);

    }

}