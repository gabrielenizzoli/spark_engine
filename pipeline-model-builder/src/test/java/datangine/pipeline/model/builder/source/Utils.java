package datangine.pipeline.model.builder.source;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.core.DataFactoryException;
import dataengine.pipeline.model.pipeline.step.Step;
import dataengine.pipeline.model.pipeline.step.StepFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Map;

public class Utils {

    @Nonnull
    public static StepFactory getStepsFactory() {
        return new StepFactory() {

            private Map<String, Step> cachedSteps;

            @Override
            public Step apply(String name) {
                if (cachedSteps == null)
                    read();
                return cachedSteps.get(name);
            }

            public void read() {
                try {
                    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                    File yamlSource = new File("src/test/resources/simplePipeline.yaml");
                    cachedSteps = mapper.readValue(yamlSource, new TypeReference<Map<String, Step>>() {
                    });
                } catch (Exception e) {
                    throw new DataFactoryException("can't build", e);
                }
            }
        };
    }

}
