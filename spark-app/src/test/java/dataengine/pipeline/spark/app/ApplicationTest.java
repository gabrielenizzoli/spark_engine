package dataengine.pipeline.spark.app;

import dataengine.pipeline.runtime.builder.datasetconsumer.GlobalCounterConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class ApplicationTest {

    @Test
    void testMain() throws IOException {

        // given
        System.setProperty("spark.master", "local[1]");
        var args = new String[]{"-p", "./src/test/resources/testPlan.yaml", "-l", "INFO"};

        // when
        Application.main(args);

        // then
        Assertions.assertEquals(1, GlobalCounterConsumer.COUNTER.get("app").get());
    }

    @Test
    @Disabled
    void testMainHelp() throws IOException {

        // given
        var args = new String[]{"-h"};

        // when
        Application.main(args);
    }

}