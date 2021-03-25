package sparkengine.plan.runtime.builder.dataset.utils;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import sparkengine.spark.sql.udf.context.UdfContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Function;

public class GlobalUdfContextFactory {

    private static UdfContextFactory UDF_CONTEXT_FACTORY = null;

    private GlobalUdfContextFactory() {
    }

    @Nonnull
    public static synchronized Optional<UdfContextFactory> get() {
        return Optional.ofNullable(UDF_CONTEXT_FACTORY);
    }

    public static synchronized UdfContextFactory init(SparkSession sparkSession) {
        if (UDF_CONTEXT_FACTORY != null)
            throw new IllegalArgumentException("Global udf factory already defined");
        UDF_CONTEXT_FACTORY = UdfContextFactory.init(sparkSession);
        return UDF_CONTEXT_FACTORY;
    }

    public static synchronized void reset() {
        UDF_CONTEXT_FACTORY = null;
    }

}
