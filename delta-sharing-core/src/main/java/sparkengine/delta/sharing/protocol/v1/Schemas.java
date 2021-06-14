package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Schemas.SchemasBuilder.class)
public class Schemas {

    @Nonnull
    List<Schema> items;
    @Nullable
    String nextPageToken;

}
