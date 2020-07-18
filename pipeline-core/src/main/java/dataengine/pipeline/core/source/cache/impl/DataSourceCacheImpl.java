package dataengine.pipeline.core.source.cache.impl;

import dataengine.pipeline.core.source.cache.DataSourceCache;
import dataengine.pipeline.core.source.cache.DataSourceCacheException;
import dataengine.pipeline.core.source.cache.DataSourceInfo;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.annotation.Nonnull;
import java.util.*;

@ToString
@EqualsAndHashCode
public class DataSourceCacheImpl implements DataSourceCache {

    private final Map<String, DataSourceInfo<?>> dataSources = new HashMap<>();

    @Override
    public <T> DataSourceCache add(@Nonnull String dataSourceName, @Nonnull DataSourceInfo<T> dataSourceInfo) throws DataSourceCacheException {
        if (dataSources.containsKey(dataSourceName)) {
            throw new DataSourceCacheException("datasource " + dataSourceName + " already defined");
        }

        // add new data and test
        Map<String, DataSourceInfo<?>> testDataSources = new HashMap<>(dataSources);
        testDataSources.put(dataSourceName, dataSourceInfo);
        Optional<Deque<String>> invalidPath = detectInvalidDirectCyclicalGraphPathFor(testDataSources, dataSourceInfo.getParentDataSourceNames());
        if (invalidPath.isPresent()) {
            throw new DataSourceCacheException.InvalidPath(String.format("datasource %s found with invalid path %s", dataSourceName, String.join("<-", invalidPath.get())));
        }
        dataSources.putAll(testDataSources);

        return this;
    }

    @Override
    public boolean contains(String dataSourceName) {
        return get(dataSourceName).isPresent();
    }

    @Override
    public Optional<DataSourceInfo<?>> get(String dataSourceName) {
        return Optional.ofNullable(dataSources.get(dataSourceName));
    }

    @Override
    public void replace(String dataSourceName, @Nonnull DataSourceInfo<?> dataSourceInfoReplacement) throws DataSourceCacheException {
        if (!dataSources.containsKey(dataSourceName))
            throw new DataSourceCacheException("datasource " + dataSourceName + " not in cache");
        Objects.requireNonNull(dataSourceInfoReplacement, "provided datasource info replacement is null");
        if (!dataSources.get(dataSourceName).isCompatible(dataSourceInfoReplacement))
            throw new DataSourceCacheException("provided datasource info [" + dataSourceInfoReplacement + "] not compatible with datasource " + dataSourceName + ": " + dataSources.get(dataSourceName));
        dataSources.put(dataSourceName, dataSourceInfoReplacement);
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
