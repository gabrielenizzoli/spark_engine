package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Value
@Builder
public class GlobalCounterConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    String key;

    public static final Map<String, AtomicLong> COUNTER = new ConcurrentHashMap<>();

    @Override
    public void readFrom(Dataset<T> dataset) throws DatasetConsumerException {
        COUNTER.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(dataset.count());
    }

}
