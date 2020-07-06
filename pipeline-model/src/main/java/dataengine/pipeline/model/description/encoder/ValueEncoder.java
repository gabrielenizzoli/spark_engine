package dataengine.pipeline.model.description.encoder;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ValueEncoder.Builder.class)
public class ValueEncoder implements Encoder {

    @Nonnull
    DataType type;

}
