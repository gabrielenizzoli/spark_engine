package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import lombok.Value;
import org.apache.spark.sql.Dataset;

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
