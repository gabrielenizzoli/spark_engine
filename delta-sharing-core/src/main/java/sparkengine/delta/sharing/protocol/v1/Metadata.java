package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Metadata.MetadataBuilder.class)
public class Metadata {

    @Nonnull
    String id;
    @Nullable
    String name;
    @Nullable
    String description;
    @Nonnull
    Format format;
    @Nonnull
    String schemaString;
    @Nonnull
    List<String> partitionColumns;

}
