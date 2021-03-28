package sparkengine.spark.transformation.context;

import lombok.Builder;
import lombok.Value;
import org.apache.spark.util.LongAccumulator;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder
public class DefaultDataTransformationContext implements DataTransformationContext {

    @Nonnull
    LongAccumulator fallbackAccumulator;
    @Nonnull
    @Builder.Default
    Map<String, LongAccumulator> accumulators = Map.of();

    @Override
    public void acc(String name, long value) {
        accumulators.getOrDefault(name, fallbackAccumulator).add(value);
    }

}
