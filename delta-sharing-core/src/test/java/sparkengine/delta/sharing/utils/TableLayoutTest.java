package sparkengine.delta.sharing.utils;

import org.junit.jupiter.api.Test;
import sparkengine.delta.sharing.model.Table;
import sparkengine.delta.sharing.model.TableMetadata;
import sparkengine.delta.sharing.model.TableName;
import sparkengine.spark.test.SparkSessionManager;

import java.util.List;


class TableLayoutTest extends SparkSessionManager {

    @Test
    void load() throws TableLayoutLoaderException {

        var table = new Table(new TableName("a", "b", "c"), new TableMetadata("/mnt/data/datasets/wiki/lake"));

        var store = TableLayoutStore.builder().sparkSession(sparkSession).build();

        var loader = store.getTableLayout(table);

        loader.streamTableProtocol(true, List.of("lang='it'")).forEach(System.out::println);
    }

}