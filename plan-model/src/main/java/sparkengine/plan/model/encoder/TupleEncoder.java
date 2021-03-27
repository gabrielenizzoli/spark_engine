package sparkengine.plan.model.encoder;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = TupleEncoder.TupleEncoderBuilder.class)
public class TupleEncoder implements DataEncoder {

    @Nonnull
    List<DataEncoder> of;

}
