package dataengine.pipeline.core.source;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
@Builder
public class DataSourceCache<T> implements dataengine.pipeline.core.source.DataSource<T> {

    @Nonnull
    dataengine.pipeline.core.source.DataSource<T> dataSource;
    @NonFinal
    Dataset<T> cachedDataset;

    @Override
    public Dataset<T> get() {
        if (cachedDataset != null) {
            return cachedDataset;
        }
        cachedDataset = dataSource.get();
        return cachedDataset;
    }

}
