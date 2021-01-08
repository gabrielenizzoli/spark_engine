package dataengine.pipeline.model.encoder;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = TupleEncoder.Builder.class)
public class TupleEncoder implements DataEncoder {

    @Nonnull
    List<DataEncoder> of;

}
