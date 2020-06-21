package dataengine.pipeline.model.pipeline.step;

import java.util.function.Function;

public interface StepFactory extends Function<String, Step> {

    default StepFactory withCache() {
        if (this instanceof StepFactoryCache)
            return this;
        return StepFactoryCache.builder().stepFactory(this).build();
    }

}
