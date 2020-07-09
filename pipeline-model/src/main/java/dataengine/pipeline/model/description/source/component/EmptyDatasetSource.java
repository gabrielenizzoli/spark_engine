package dataengine.pipeline.model.description.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.encoder.Encoder;
import dataengine.pipeline.model.description.source.SourceComponent;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = EmptyDatasetSource.Builder.class)
public class EmptyDatasetSource implements SourceComponent {

    @Nonnull
    Encoder as;

}
