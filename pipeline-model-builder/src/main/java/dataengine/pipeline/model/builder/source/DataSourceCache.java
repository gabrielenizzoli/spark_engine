package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceError;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.model.description.source.Component;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.*;

@NoArgsConstructor
@Builder
public class DataSourceCache {

    @Nonnull
    @Singular
    private final Map<String, DataSourceInfo<?>> dataSources = new HashMap<>();

    @Value
    @Builder
    public static class DataSourceInfo<T> {

        @Nonnull
        DataSource<T> dataSource;
        @Nonnull
        Component component;
        @Nonnull
        @Singular
        Set<String> parentDataSourceNames;

    }

    @Value
    private class DataSourceCacheReference<T> implements DataSource<T> {

        @Nonnull
        String dataSourceName;

        @Override
        @SuppressWarnings("unchecked")
        public Dataset<T> get() {
            if (!dataSources.containsKey(dataSourceName)) {
                throw new DataSourceError("datasource " + dataSourceName + " not found");
            }
            return (Dataset<T>) dataSources.get(dataSourceName).getDataSource().get();
        }

    }

    public <T> DataSource<T> getDataSourceCacheReference(@Nonnull String dataSourceName) {
        return new DataSourceCacheReference<>(dataSourceName);
    }

    public <T> DataSourceCache add(@Nonnull String dataSourceName, @Nonnull DataSourceInfo<T> dataSourceInfo) throws DataSourceCatalogException {
        if (dataSources.containsKey(dataSourceName)) {
            throw new DataSourceCatalogException("datasource " + dataSourceName + " already defined");
        }

        // add new data and test copy
        Map<String, DataSourceInfo<?>> testDataSources = new HashMap<>(dataSources);
        testDataSources.put(dataSourceName, dataSourceInfo);
        Optional<Deque<String>> invalidPath = detectInvalidDirectCyclicalGraphPathFor(testDataSources, dataSourceInfo.getParentDataSourceNames());
        if (invalidPath.isPresent()) {
            throw new DataSourceCatalogException.InvalidPath(String.format("datasource %s found with invalid path %s", dataSourceName, String.join("<-", invalidPath.get())));
        }
        dataSources.putAll(testDataSources);

        return this;
    }

    public Optional<DataSourceInfo<?>> get(String dataSourceName) {
        return Optional.ofNullable(dataSources.get(dataSourceName));
    }

    private static Optional<Deque<String>> detectInvalidDirectCyclicalGraphPathFor(Map<String, DataSourceInfo<?>> dataSources, Set<String> parentNames) {

        if (parentNames.isEmpty()) {
            return Optional.empty();
        }

        // prepare starting list of paths to monitor for current
        Queue<Deque<String>> queueOfTestPaths = new LinkedList<>();
        for (String parentName : parentNames) {
            Deque<String> startPath = new LinkedList<>();
            startPath.push(parentName);
            queueOfTestPaths.add(startPath);
        }

        while (!queueOfTestPaths.isEmpty()) {

            List<Deque<String>> newTestPaths = new LinkedList<>();

            // given testPath, expand set of paths to test
            Deque<String> testPath = queueOfTestPaths.remove();
            String latestDataSourceName = testPath.peek();
            dataSources.forEach((childName, info) -> {
                if (info.getParentDataSourceNames().contains(latestDataSourceName)) {
                    Deque<String> childPathCopy = new LinkedList<>(testPath);
                    childPathCopy.push(childName);
                    newTestPaths.add(childPathCopy);
                }
            });

            // test: if a path has duplicates, the path is not valid
            for (Deque<String> path : newTestPaths) {
                if (path.stream().distinct().count() != path.size()) {
                    return Optional.of(path);
                }
            }

            // queue paths to monitor, and continue
            queueOfTestPaths.addAll(newTestPaths);
        }

        return Optional.empty();
    }

}
