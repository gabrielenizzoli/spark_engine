package dataengine.pipeline.model.description.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.encoder.DataEncoder;
import dataengine.pipeline.model.description.source.EncodedComponent;
import dataengine.pipeline.model.description.source.SourceComponent;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = EmptyDatasetSource.Builder.class)
public class EmptyDatasetSource implements SourceComponent, EncodedComponent {

    @Nonnull
    DataEncoder encodedAs;

}
