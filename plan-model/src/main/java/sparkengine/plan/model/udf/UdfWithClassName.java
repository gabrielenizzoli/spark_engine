package sparkengine.plan.model.udf;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = UdfWithClassName.UdfWithClassNameBuilder.class)
public class UdfWithClassName implements Udf {

    @Nonnull
    String className;
    @Nullable
    Map<String, String> accumulators;

}
