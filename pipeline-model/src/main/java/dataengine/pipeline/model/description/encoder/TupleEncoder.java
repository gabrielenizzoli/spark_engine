package dataengine.pipeline.model.description.encoder;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = TupleEncoder.Builder.class)
public class TupleEncoder implements Encoder {

    @Nonnull
    List<Encoder> of;

}
