package dataengine.pipeline.model.supplier;

import dataengine.pipeline.model.pipeline.step.Step;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

class StepFactoryYamlMapTest {

    @Test
    void testYaml() {

        File yamlSource = new File("src/test/resources/simplePipeline.yaml");
        StepFactoryYamlMap factory = StepFactoryYamlMap.builder().inputStreamFactory(() -> {
            try {
                return new FileInputStream(yamlSource);
            } catch (FileNotFoundException e) {
                throw new IllegalStateException();
            }
        }).build();

        Step step = factory.apply("tx");

        Assertions.assertNotNull(step);
    }

}