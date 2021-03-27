package sparkengine.plan.model.udf;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = UdfWithScalaScript.UdfWithScalaScriptBuilder.class)
public class UdfWithScalaScript implements Udf {

    @Nonnull
    String name;
    @Nonnull
    String scala;
    @Nullable
    Map<String, String> accumulators;

}
