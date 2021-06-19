package sparkengine.delta.sharing.protocol.v1;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = TableQuery.TableQueryBuilder.class)
public class TableQuery {

    @Nullable
    List<String> predicateHints;
    @Nullable
    Long limitHint;

}
