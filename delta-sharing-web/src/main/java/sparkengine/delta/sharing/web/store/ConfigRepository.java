package sparkengine.delta.sharing.web.store;

import org.jooq.DSLContext;
import org.jooq.ResultQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import sparkengine.delta.sharing.db.hsqldb.jooq.tables.records.ShareConfigRecord;

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

    public Stream<String> streamAllShareNames(Optional<Page> page) {

        var baseQuery = dsl.selectFrom(SHARE_CONFIG).orderBy(SHARE_CONFIG.NAME);
        var query = page
                .map(p -> (ResultQuery<ShareConfigRecord>)baseQuery.offset(p.getStart()).limit(p.getSize()))
                .orElse(baseQuery);

        return dsl.fetchStream(query).map(ShareConfigRecord::getName);
    }

    public boolean addShare(String shareName) {
        return dsl.insertInto(SHARE_CONFIG).columns(SHARE_CONFIG.NAME).values(shareName).execute() == 1;
    }

    public boolean isShare(String shareName) {
        var query = dsl
                .select(SHARE_CONFIG.NAME)
                .from(SHARE_CONFIG)
                .where(SHARE_CONFIG.NAME.eq(shareName));
        return dsl.fetchExists(query);
    }

    public boolean isSchema(String shareName, String schemaName) {
        var query = dsl
                .select(SCHEMA_CONFIG.NAME)
                .from(SCHEMA_CONFIG)
                .where(SCHEMA_CONFIG.SHARE_NAME.eq(shareName).and(SCHEMA_CONFIG.NAME.eq(schemaName)));
        return dsl.fetchExists(query);
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


}
