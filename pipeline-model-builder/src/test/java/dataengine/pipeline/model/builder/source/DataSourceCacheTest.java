package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.source.Component;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

class DataSourceCacheTest {

    @Test
    void testDAG_1Node() throws DataSourceCatalogException {

        new DataSourceCache()
                .add("ds1", withParents());
    }

    @Test
    void testDAG_2Nodes() throws DataSourceCatalogException {

        new DataSourceCache()
                .add("ds1", withParents("ds2"))
                .add("ds2", withParents());

    }

    @Test
    void testDAG_2NodesWithCycle() {

        Assertions.assertThrows(DataSourceCatalogException.class, () -> {
            new DataSourceCache()
                    .add("ds1", withParents("ds2"))
                    .add("ds2", withParents("ds1"));
        });

    }

    @Test
    void testDAG_validComplexGraph() throws DataSourceCatalogException {

        new DataSourceCache()
                .add("ds0", withParents("ds1", "ds3"))
                .add("ds1", withParents("ds2", "ds3"))
                .add("ds2", withParents("ds3"))
                .add("ds3", withParents());

    }

    @Test
    void testDAG_invalidComplexGraph() {

        Assertions.assertThrows(DataSourceCatalogException.class, () -> {
            new DataSourceCache()
                    .add("ds0", withParents("ds1", "ds3"))
                    .add("ds1", withParents("ds2", "ds3"))
                    .add("ds2", withParents("ds3"))
                    .add("ds3", withParents("ds1"));
        });

    }

    private static DataSourceCache.DataSourceInfo<Object> withParents(String... names) {
        return DataSourceCache.DataSourceInfo.builder()
                .dataSource(() -> null)
                .component(new Component() {
                })
                .parentDataSourceNames(Arrays.stream(names).collect(Collectors.toList()))
                .build();
    }

}