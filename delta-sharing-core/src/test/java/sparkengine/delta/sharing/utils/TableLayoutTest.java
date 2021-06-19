package sparkengine.delta.sharing.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.delta.sharing.model.Table;
import sparkengine.delta.sharing.model.TableMetadata;
import sparkengine.delta.sharing.model.TableName;
import sparkengine.spark.test.SparkSessionManager;

import java.util.List;


class TableLayoutTest extends SparkSessionManager {

    @Test
    void testLoadAll() throws TableLayoutLoaderException {

        // given
        var table = new Table(new TableName("a", "b", "c"), new TableMetadata("./src/test/resources/delta-table"));
        var store = TableLayoutStore.builder().sparkSession(sparkSession).build();
        var loader = store.getTableLayout(table);

        // when
        var files = loader.streamTableFilesProtocol();

        // then
        Assertions.assertEquals(16, files.count());
    }

    @Test
    void testLoadWithHints() throws TableLayoutLoaderException {

        // given
        var table = new Table(new TableName("a", "b", "c"), new TableMetadata("./src/test/resources/delta-table"));
        var store = TableLayoutStore.builder().sparkSession(sparkSession).build();
        var loader = store.getTableLayout(table);

        // when
        var files = loader.streamTableProtocol(true, List.of("key1='a'"));

        // then
        Assertions.assertEquals(8, files.count());
    }

}