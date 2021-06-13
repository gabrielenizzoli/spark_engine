package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = File.FileBuilder.class)
public class File {

    @Nonnull
    String url;
    @Nonnull
    String id;
    @Nonnull
    Map<String, String> partitionValues;
    @Nonnull
    long size;
    @Nullable
    String stats;

}
