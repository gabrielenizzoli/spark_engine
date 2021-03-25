package sparkengine.spark.sql.udf.context;

import lombok.Builder;
import lombok.Value;
import org.apache.spark.util.LongAccumulator;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder
public class DefaultUdfContext implements UdfContext {

    @Nonnull
    LongAccumulator fallbackAccumulator;
    @Nonnull
    Map<String, LongAccumulator> accumulators;

    @Override
    public void acc(String name, long value) {
        accumulators.getOrDefault(name, fallbackAccumulator).add(value);
    }

}
