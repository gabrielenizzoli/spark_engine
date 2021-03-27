package sparkengine.plan.app.runner;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.metrics.source.Source;
import sparkengine.plan.runtime.builder.RuntimeContext;

import javax.annotation.Nonnull;

@Value
@Builder
public class MetricSource implements Source {

    @Nonnull
    @Builder.Default
    String sourceName = "plan";
    @Nonnull
    @Builder.Default
    String fallbackMetricName = "unknown";
    @Nonnull
    @Builder.Default
    MetricRegistry metricRegistry = new MetricRegistry();

    public static MetricSource buildWithDefaults() {
        return MetricSource.builder().build();
    }

    @Override
    public String sourceName() {
        return sourceName;
    }

    @Override
    public MetricRegistry metricRegistry() {
        return metricRegistry;
    }

    public MetricSource withRuntimeAccumulators(RuntimeContext runtimeContext) {
        metricRegistry.register(fallbackMetricName, (Gauge<Long>) runtimeContext.getFallbackAccumulator()::value);
        runtimeContext.getAccumulatorMap().forEach((name, accumulator) -> metricRegistry.register(name, (Gauge<Long>) accumulator::value));
        return this;
    }

}
