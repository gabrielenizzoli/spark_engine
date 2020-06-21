package dataengine.pipeline.core.source;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
@RequiredArgsConstructor(staticName = "of")
public class DataSourceStore<T> implements DataSource<T> {

    @Nonnull
    DataSource<T> dataSource;
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
