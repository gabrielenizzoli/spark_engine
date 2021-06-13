package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nullable;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Wrapper.WrapperBuilder.class)
public class Wrapper {

    @Nullable
    Protocol protocol;
    @Nullable
    Metadata metaData;
    @Nullable
    File file;

}
