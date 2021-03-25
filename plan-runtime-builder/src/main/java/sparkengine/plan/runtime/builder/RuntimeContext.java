package sparkengine.plan.runtime.builder;

import lombok.Value;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import sparkengine.plan.runtime.builder.dataset.utils.UdfContextFactory;
import sparkengine.spark.sql.udf.context.DefaultUdfContext;
import sparkengine.spark.sql.udf.context.UdfContext;
import sparkengine.spark.utils.SparkUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Value
public class RuntimeContext {

    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    LongAccumulator fallbackAccumulator;
    @Nonnull
    Map<String, LongAccumulator> accumulatorMap;

    public static RuntimeContext init(SparkSession sparkSession) {
        return new RuntimeContext(sparkSession, SparkUtils.longAnonymousAccumulator(sparkSession), new ConcurrentHashMap<>());
    }

    public LongAccumulator getOrCreateAccumulator(String name) {
        return accumulatorMap.computeIfAbsent(name, n -> SparkUtils.longAccumulator(sparkSession, n));
    }

    public UdfContext buildUdfContext(@Nullable Map<String, String> accumulatorNamesRemap) {
        var newAccumulatorMap = new HashMap<String, LongAccumulator>();
        Optional.ofNullable(accumulatorNamesRemap)
                .orElse(Map.of())
                .forEach((internalName, externalName) -> newAccumulatorMap.put(internalName, getOrCreateAccumulator(externalName)));
        return DefaultUdfContext.builder().fallbackAccumulator(fallbackAccumulator).accumulators(newAccumulatorMap).build();
    }

    public Broadcast<UdfContext> buildBroadcastUdfContext(@Nullable Map<String, String> accumulatorNamesRemap) {
        return SparkUtils.broadcast(sparkSession, buildUdfContext(accumulatorNamesRemap));
    }

}
