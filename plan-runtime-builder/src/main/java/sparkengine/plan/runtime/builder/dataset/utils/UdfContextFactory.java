package sparkengine.plan.runtime.builder.dataset.utils;

import lombok.Value;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import sparkengine.spark.sql.udf.context.DefaultUdfContext;
import sparkengine.spark.sql.udf.context.UdfContext;
import sparkengine.spark.utils.SparkUtils;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Value
public class UdfContextFactory {

    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    LongAccumulator fallbackAccumulator;
    @Nonnull
    Map<String, LongAccumulator> accumulatorMap;

    public static UdfContextFactory init(SparkSession sparkSession) {
      return new UdfContextFactory(sparkSession, SparkUtils.longAnonymousAccumulator(sparkSession), new ConcurrentHashMap<>());
    }

    public LongAccumulator getOrCreateAccumulator(String name) {
        return accumulatorMap.computeIfAbsent(name, n -> SparkUtils.longAccumulator(sparkSession, n));
    }

    public UdfContext buildUdfContext(Map<String, String> accumulatorNamesRemap) {
        var newAccumulatorMap = new HashMap<String, LongAccumulator>();
        accumulatorNamesRemap.forEach((internalName, externalName) -> newAccumulatorMap.put(internalName, getOrCreateAccumulator(externalName)));
        return DefaultUdfContext.builder().fallbackAccumulator(fallbackAccumulator).accumulators(newAccumulatorMap).build();
    }

    public Broadcast<UdfContext> buildBroadcastUdfContext(Map<String, String> accumulatorNamesRemap) {
        return SparkUtils.broadcast(sparkSession, buildUdfContext(accumulatorNamesRemap));
    }


}
