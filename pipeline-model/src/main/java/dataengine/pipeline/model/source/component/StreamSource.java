package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.source.EncodedComponent;
import dataengine.pipeline.model.source.SourceComponent;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = StreamSource.Builder.class)
public class StreamSource implements SourceComponent, EncodedComponent {

    @Nonnull
    String format;
    @Nonnull
    Map<String, String> options;
    @Nullable
    DataEncoder encodedAs;

}
