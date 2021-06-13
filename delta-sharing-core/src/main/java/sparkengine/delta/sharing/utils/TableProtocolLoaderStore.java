package sparkengine.delta.sharing.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import sparkengine.delta.sharing.model.Table;
import sparkengine.delta.sharing.model.TableName;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;

@Value
@Builder
public class TableProtocolLoaderStore {

    @Nonnull
    @Builder.Default
    Cache<TableName, TableProtocolLoader> sharingTablesCache = CacheBuilder.newBuilder().build();
    @Nonnull
    SparkSession sparkSession;

    public TableProtocolLoader getTableProtocolLoader(Table table) throws ExecutionException {
        return sharingTablesCache.get(table.getTableName(), () -> new TableProtocolLoader(DeltaLog.forTable(sparkSession, table.getTableMetadata().getLocation())));
    }

}
