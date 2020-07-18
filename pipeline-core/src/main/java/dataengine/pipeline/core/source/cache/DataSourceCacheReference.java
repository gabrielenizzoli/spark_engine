package dataengine.pipeline.core.source.cache;


import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceError;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
public class DataSourceCacheReference<T> implements DataSource<T> {

    @Nonnull
    String dataSourceName;
    @Nonnull
    DataSourceCache dataSourceCache;

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<T> get() {
        return (Dataset<T>)dataSourceCache.get(dataSourceName)
                .orElseThrow(() -> new DataSourceError("datasource " + dataSourceName + " not found in cache"))
                .getDataSource().get();
    }

}
