package dataengine.pipeline.core.source.cache;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface DataSourceCache {

    <T> DataSourceCache add(@Nonnull String dataSourceName,
                            @Nonnull DataSourceInfo<T> dataSourceInfo)
            throws DataSourceCacheException;

    boolean contains(String dataSourceName);

    Optional<DataSourceInfo<?>> get(String dataSourceName);

    void replace(String dataSourceName,
                 @Nonnull DataSourceInfo<?> dataSourceInfoReplacement)
            throws DataSourceCacheException;

}
