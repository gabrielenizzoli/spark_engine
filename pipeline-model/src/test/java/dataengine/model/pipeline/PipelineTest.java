package dataengine.model.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

class PipelineTest {

    @Test
    public void testSample() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        File yamlSource = new File("src/test/resources/sample.yaml");
        Pipeline pipeline = mapper.readValue(yamlSource, Pipeline.class);
    }

}