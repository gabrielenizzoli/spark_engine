package sparkengine.delta.sharing.model;

import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class TableMetadata {

    @Nonnull
    String location;

}
