package sparkengine.delta.sharing.utils;

import org.junit.jupiter.api.Test;
import sparkengine.delta.sharing.model.Table;
import sparkengine.delta.sharing.model.TableMetadata;
import sparkengine.delta.sharing.model.TableName;
import sparkengine.spark.test.SparkSessionManager;

import java.util.List;
import java.util.concurrent.ExecutionException;


class TableProtocolLoaderTest extends SparkSessionManager {

    @Test
    void load() throws ExecutionException {

        var table = new Table(new TableName("a", "b", "c"), new TableMetadata("/mnt/data/datasets/wiki/lake"));

        var store = TableProtocolLoaderStore.builder().sparkSession(sparkSession).build();

        var loader = store.getTableProtocolLoader(table);

        loader.loadTableProtocol(true, List.of("lang='it'")).forEach(System.out::println);
    }

}