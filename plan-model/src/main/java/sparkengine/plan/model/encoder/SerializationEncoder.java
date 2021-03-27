package sparkengine.plan.model.encoder;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SerializationEncoder.SerializationEncoderBuilder.class)
public class SerializationEncoder implements DataEncoder {

    public enum SerializationVariant {
        JAVA,
        KRYO
    }

    @Nonnull
    SerializationVariant variant;
    @Nonnull
    DataType ofClass;

}
