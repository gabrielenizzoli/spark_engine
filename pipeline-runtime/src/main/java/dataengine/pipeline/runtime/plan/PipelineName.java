package dataengine.pipeline.runtime.plan;

import lombok.Value;

import javax.annotation.Nonnull;

@Value(staticConstructor = "of")
public class PipelineName {

    @Nonnull
    String source;
    @Nonnull
    String destination;

}
