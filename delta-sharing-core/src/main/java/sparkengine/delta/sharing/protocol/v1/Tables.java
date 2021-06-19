package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Tables.TablesBuilder.class)
public class Tables {

    @Nonnull
    List<Table> items;
    @Nullable
    String nextPageToken;

}
