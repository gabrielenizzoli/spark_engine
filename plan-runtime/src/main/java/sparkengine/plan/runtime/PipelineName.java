package sparkengine.plan.runtime;

import lombok.Value;

import javax.annotation.Nonnull;

@Value(staticConstructor = "of")
public class PipelineName {

    @Nonnull
    String dataset;
    @Nonnull
    String consumer;

}
