package sparkengine.delta.sharing.web.store;

import org.jooq.DSLContext;
import org.jooq.ResultQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import sparkengine.delta.sharing.db.hsqldb.jooq.tables.records.SchemaConfigRecord;
import sparkengine.delta.sharing.db.hsqldb.jooq.tables.records.ShareConfigRecord;
import sparkengine.delta.sharing.db.hsqldb.jooq.tables.records.TableConfigRecord;
import sparkengine.delta.sharing.model.TableMetadata;
import sparkengine.delta.sharing.model.TableName;

import java.util.Optional;
import java.util.stream.Stream;

import static sparkengine.delta.sharing.db.hsqldb.jooq.Tables.*;

@Repository
public class ConfigRepository {

    private final DSLContext dsl;

    @Autowired
    public ConfigRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public boolean isShare(String shareName) {
        var query = dsl
                .select(SHARE_CONFIG.NAME)
                .from(SHARE_CONFIG)
                .where(SHARE_CONFIG.NAME.eq(shareName));
        return dsl.fetchExists(query);
    }

    public boolean addShare(String shareName) {
        return dsl.insertInto(SHARE_CONFIG).columns(SHARE_CONFIG.NAME).values(shareName).execute() == 1;
    }

    public Stream<String> streamShareNames(Optional<Page> page) {

        var baseQuery = dsl.selectFrom(SHARE_CONFIG).orderBy(SHARE_CONFIG.NAME);
        var query = page
                .map(p -> (ResultQuery<ShareConfigRecord>)baseQuery.offset(p.getStart()).limit(p.getSize()))
                .orElse(baseQuery);

        return dsl.fetchStream(query).map(ShareConfigRecord::getName);
    }

    public boolean isSchema(String shareName, String schemaName) {
        var query = dsl
                .select(SCHEMA_CONFIG.NAME)
                .from(SCHEMA_CONFIG)
                .where(SCHEMA_CONFIG.SHARE_NAME.eq(shareName).and(SCHEMA_CONFIG.NAME.eq(schemaName)));
        return dsl.fetchExists(query);
    }

    public boolean addSchema(String shareName, String schemaName) {
        return dsl.insertInto(SCHEMA_CONFIG).columns(SCHEMA_CONFIG.SHARE_NAME, SCHEMA_CONFIG.NAME).values(shareName, schemaName).execute() == 1;
    }

    public Stream<String> streamSchemaNames(String shareName, Optional<Page> page) {
        var baseQuery = dsl
                .selectFrom(SCHEMA_CONFIG)
                .where(SCHEMA_CONFIG.SHARE_NAME.eq(shareName))
                .orderBy(SCHEMA_CONFIG.NAME);
        var query = page
                .map(p -> (ResultQuery<SchemaConfigRecord>)baseQuery.offset(p.getStart()).limit(p.getSize()))
                .orElse(baseQuery);
        return dsl.fetchStream(query).map(SchemaConfigRecord::getName);
    }

    public boolean isTable(String shareName, String schemaName, String tableName) {
        var query = dsl
                .select(TABLE_CONFIG.NAME)
                .from(TABLE_CONFIG)
                .where(TABLE_CONFIG.SHARE_NAME.eq(shareName)
                        .and(TABLE_CONFIG.SCHEMA_NAME.eq(schemaName)
                                .and(TABLE_CONFIG.NAME.eq(tableName))));
        return dsl.fetchExists(query);
    }

    public TableMetadata getTableMetadata(TableName tableName) {
        var condition = TABLE_CONFIG.SHARE_NAME.eq(tableName.getShare())
                .and(TABLE_CONFIG.SCHEMA_NAME.eq(tableName.getSchema())
                        .and(TABLE_CONFIG.NAME.eq(tableName.getTable())));
        var data = dsl.fetchOne(TABLE_CONFIG, condition);

        return new TableMetadata(data.getLocation());
    }

    public boolean addTable(String shareName, String schemaName, String tableName) {
        return dsl.insertInto(TABLE_CONFIG).columns(TABLE_CONFIG.SHARE_NAME, TABLE_CONFIG.SCHEMA_NAME, TABLE_CONFIG.NAME).values(shareName, schemaName, tableName).execute() == 1;
    }

    public Stream<String> streamTableNames(String shareName, String schemaName, Optional<Page> page) {
        var baseQuery = dsl
                .selectFrom(TABLE_CONFIG)
                .where(TABLE_CONFIG.SHARE_NAME.eq(shareName).and(TABLE_CONFIG.SCHEMA_NAME.eq(schemaName)))
                .orderBy(TABLE_CONFIG.NAME);
        var query = page
                .map(p -> (ResultQuery<TableConfigRecord>)baseQuery.offset(p.getStart()).limit(p.getSize()))
                .orElse(baseQuery);
        return dsl.fetchStream(query).map(TableConfigRecord::getName);
    }

}
