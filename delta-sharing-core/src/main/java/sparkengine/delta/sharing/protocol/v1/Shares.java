package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Shares.SharesBuilder.class)
public class Shares {

    @Nonnull
    List<Share> items;
    @Nullable
    String nextPageToken;

}
