package sparkengine.spark.sql.udf.context;

import lombok.Builder;
import lombok.Value;
import org.apache.spark.util.LongAccumulator;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder(toBuilder = true)
public class DefaultUdfContext implements UdfContext {

    @Nonnull
    Map<String, LongAccumulator> accumulators;
    @Nonnull
    LongAccumulator fallbackAccumulator;

    @Override
    public void acc(String name, long value) {
        accumulators.getOrDefault(name, fallbackAccumulator).add(value);
    }

}
