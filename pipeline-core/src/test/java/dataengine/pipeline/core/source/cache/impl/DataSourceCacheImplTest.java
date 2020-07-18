package dataengine.pipeline.core.source.cache.impl;

import dataengine.pipeline.core.source.cache.DataSourceCacheException;
import dataengine.pipeline.core.source.cache.DataSourceInfo;
import dataengine.pipeline.core.source.cache.impl.DataSourceCacheImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

class DataSourceCacheImplTest {

    @Test
    void testDAG_1Node() throws DataSourceCacheException {

        new DataSourceCacheImpl()
                .add("ds1", withParents());
    }

    @Test
    void testDAG_2Nodes() throws DataSourceCacheException {

        new DataSourceCacheImpl()
                .add("ds1", withParents("ds2"))
                .add("ds2", withParents());

    }

    @Test
    void testDAG_2NodesWithCycle() {

        Assertions.assertThrows(DataSourceCacheException.class, () -> {
            new DataSourceCacheImpl()
                    .add("ds1", withParents("ds2"))
                    .add("ds2", withParents("ds1"));
        });

    }

    @Test
    void testDAG_validComplexGraph() throws DataSourceCacheException {

        new DataSourceCacheImpl()
                .add("ds0", withParents("ds1", "ds3"))
                .add("ds1", withParents("ds2", "ds3"))
                .add("ds2", withParents("ds3"))
                .add("ds3", withParents());

    }

    @Test
    void testDAG_invalidComplexGraph() {

        Assertions.assertThrows(DataSourceCacheException.class, () -> {
            new DataSourceCacheImpl()
                    .add("ds0", withParents("ds1", "ds3"))
                    .add("ds1", withParents("ds2", "ds3"))
                    .add("ds2", withParents("ds3"))
                    .add("ds3", withParents("ds1"));
        });

    }

    private static DataSourceInfo<Object> withParents(String... names) {
        return DataSourceInfo.builder()
                .dataSource(() -> null)
                .parentDataSourceNames(Arrays.stream(names).collect(Collectors.toList()))
                .build();
    }

}