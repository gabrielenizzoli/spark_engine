package sparkengine.plan.runtime.builder.datasetconsumer;

import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
@Builder
public class ShowConsumer<T> implements DatasetConsumer<T> {

    int count;
    int truncate;

    @Override
    public void readFrom(Dataset<T> dataset) {
        dataset.show(count, truncate);
    }

}
