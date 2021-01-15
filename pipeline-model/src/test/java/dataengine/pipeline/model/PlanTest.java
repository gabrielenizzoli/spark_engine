package dataengine.pipeline.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.pipeline.Plan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

class PlanTest {

    @Test
    public void testSampleSerialization() throws IOException {
        // given
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File yamlSource = new File("src/test/resources/sample.yaml");

        // when
        Plan plan = mapper.readValue(yamlSource, Plan.class);

        // then
        Assertions.assertNotNull(plan);
        Assertions.assertNotNull(plan.getSinks());
        Assertions.assertTrue(plan.getSinks().size() > 0);
        Assertions.assertNotNull(plan.getComponents());
        Assertions.assertTrue(plan.getComponents().size() > 0);

    }

}