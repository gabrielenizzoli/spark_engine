package dataengine.pipeline.model.pipeline.step;

import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder
public class StepFactoryCache implements StepFactory {

    @Nonnull
    StepFactory stepFactory;
    @Nonnull
    @lombok.Builder.Default
    Map<String, Step> cachedSteps = new HashMap<>();

    @Override
    public Step apply(String name) {
        return cachedSteps.computeIfAbsent(name, stepFactory);
    }
}
