package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Table.TableBuilder.class)
public class Table {

    @Nonnull
    String name;
    @Nonnull
    String schema;
    @Nonnull
    String share;

}
