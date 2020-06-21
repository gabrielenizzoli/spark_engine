package dataengine.pipeline.model.supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.pipeline.step.Step;
import dataengine.pipeline.model.pipeline.step.StepFactory;
import dataengine.pipeline.model.pipeline.step.StepFactoryException;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.NonFinal;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;

@Value
@Builder
public class StepFactoryYamlMap implements StepFactory {

    @Nonnull
    Supplier<InputStream> inputStreamFactory;
    @NonFinal
    Map<String, Step> cachedSteps = null;

    @Override
    public Step apply(String name) {
        if (cachedSteps == null)
            read();
        return cachedSteps.get(name);
    }

    public void read() {
        try(InputStream inputStream = inputStreamFactory.get()) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            cachedSteps = mapper.readValue(inputStream, new TypeReference<Map<String, Step>>() {
            });
        } catch (Exception e) {
            throw new StepFactoryException("can't build", e);
        }
    }

}
