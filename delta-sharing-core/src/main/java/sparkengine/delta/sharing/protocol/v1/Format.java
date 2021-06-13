package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Format.FormatBuilder.class)
public class Format {

    enum Provider {
        parquet
    }

    @Nonnull
    @Builder.Default
    Provider provider = Provider.parquet;

}
