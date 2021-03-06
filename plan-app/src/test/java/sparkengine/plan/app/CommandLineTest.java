package sparkengine.plan.app;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.runtime.builder.datasetconsumer.GlobalCounterConsumer;
import sparkengine.spark.test.SparkSessionManager;

import java.util.Optional;

class CommandLineTest {

    @Test
    void testMain() throws Throwable {

        // given
        System.setProperty("spark.master", "local[1]");
        SparkSessionManager.SPARK_TEST_PROPERTIES.forEach(System::setProperty);
        Optional.ofNullable(GlobalCounterConsumer.COUNTER.get("app")).ifPresent(c -> c.set(0));
        var args = new String[]{"-p", "./src/test/resources/testPlan.yaml", "-l", "INFO"};

        // when
        Start.main(args);

        // then
        Assertions.assertEquals(1, GlobalCounterConsumer.COUNTER.get("app").get());
    }

    @Test
    void testHelp() throws Throwable {

        // given
        System.setProperty("spark.master", "local[1]");
        SparkSessionManager.SPARK_TEST_PROPERTIES.forEach(System::setProperty);
        var args = new String[]{"--help"};

        // when
        Start.main(args);
    }


}