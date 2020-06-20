package dataengine.pipeline.model.pipeline.encoder;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = BeanEncoder.Builder.class)
public class BeanEncoder implements Encoder {

    @Nonnull
    String ofClass;

}
