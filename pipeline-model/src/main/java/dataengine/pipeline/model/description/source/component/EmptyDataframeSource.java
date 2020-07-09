package dataengine.pipeline.model.description.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.encoder.Encoder;
import dataengine.pipeline.model.description.source.SourceComponent;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = EmptyDataframeSource.Builder.class)
public class EmptyDataframeSource implements SourceComponent {

    @Nonnull
    String schema;

}
