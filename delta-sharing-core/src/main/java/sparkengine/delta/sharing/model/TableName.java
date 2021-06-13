package sparkengine.delta.sharing.model;

import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class TableName {

    @Nonnull
    String share;
    @Nonnull
    String schema;
    @Nonnull
    String table;

}
