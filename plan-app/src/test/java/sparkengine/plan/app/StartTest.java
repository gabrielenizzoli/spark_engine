package sparkengine.plan.app;

import sparkengine.plan.runtime.builder.datasetconsumer.GlobalCounterConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class StartTest {

    @Test
    void testMain() throws IOException {

        // given
        System.setProperty("spark.master", "local[1]");
        var args = new String[]{"-p", "./src/test/resources/testPlan.yaml", "-l", "INFO"};

        // when
        Start.main(args);

        // then
        Assertions.assertEquals(1, GlobalCounterConsumer.COUNTER.get("app").get());
    }

}