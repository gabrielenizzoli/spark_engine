package dataengine.pipeline.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.pipeline.Pipelines;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

class PipelinesTest {

    @Test
    public void testSampleSerialization() throws IOException {
        // given
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File yamlSource = new File("src/test/resources/sample.yaml");

        // when
        Pipelines pipelines = mapper.readValue(yamlSource, Pipelines.class);

        // then
        Assertions.assertNotNull(pipelines);
        Assertions.assertNotNull(pipelines.getSinks());
        Assertions.assertTrue(pipelines.getSinks().size() > 0);
        Assertions.assertNotNull(pipelines.getComponents());
        Assertions.assertTrue(pipelines.getComponents().size() > 0);

    }

}