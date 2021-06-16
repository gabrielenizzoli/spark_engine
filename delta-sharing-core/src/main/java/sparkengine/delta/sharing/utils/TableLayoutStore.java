package sparkengine.delta.sharing.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import sparkengine.delta.sharing.model.Table;
import sparkengine.delta.sharing.model.TableMetadata;
import sparkengine.delta.sharing.model.TableName;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;

@Value
@Builder
public class TableLayoutStore {

    public interface TableMetadataFetcher {
        TableMetadata call(TableName tableName) throws TableLayoutLoaderException;
    }

    @Nonnull
    @Builder.Default
    Cache<TableName, TableLayout> sharingTablesCache = CacheBuilder.newBuilder().build();
    @Nonnull
    SparkSession sparkSession;

    public TableLayout getTableLayout(TableName tableName, TableMetadataFetcher metadataFetcher) throws TableLayoutLoaderException {
        try {
            return sharingTablesCache.get(tableName, () -> {
                var tableMetadata = metadataFetcher.call(tableName);
                return new TableLayout(DeltaLog.forTable(sparkSession, tableMetadata.getLocation()));
            });
        } catch (ExecutionException e) {
            throw new TableLayoutLoaderException("unable to load table protocol for " + tableName, e);
        }
    }

    public TableLayout getTableLayout(Table table) throws TableLayoutLoaderException {
        try {
            return sharingTablesCache.get(table.getTableName(), () -> new TableLayout(DeltaLog.forTable(sparkSession, table.getTableMetadata().getLocation())));
        } catch (ExecutionException e) {
            throw new TableLayoutLoaderException("unable to load table protocol for " + table.getTableName(), e);
        }
    }

}
