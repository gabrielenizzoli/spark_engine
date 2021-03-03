package sparkengine.plan.runtime.builder.datasetconsumer;

import lombok.Value;
import org.apache.spark.sql.Dataset;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumer;

import javax.annotation.Nonnull;

@Value(staticConstructor = "of")
public class ViewConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    String name;

    @Override
    public void readFrom(Dataset<T> dataset) {
        dataset.createOrReplaceTempView(name);
    }

}
