package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.source.EncodedComponent;
import dataengine.pipeline.model.source.SourceComponent;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = EmptySource.Builder.class)
public class EmptySource implements SourceComponent, EncodedComponent {

    @Nonnull
    DataEncoder encodedAs;

}
