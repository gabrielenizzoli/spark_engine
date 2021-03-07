package sparkengine.spark.sql.logicalplan.tablelist;

import org.junit.jupiter.api.Test;
import sparkengine.spark.sql.logicalplan.PlanExplorerException;
import sparkengine.spark.test.SparkSessionManager;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableListExplorerTest extends SparkSessionManager {

    @Test
    void exploreLogicalPlan() throws PlanExplorerException {

        // given
        var sql = "select * from (select value+1 as valueWithOperation, (select value from table1) as valueSubquery from table2)";

        // when
        var set = TableListExplorer.findTableListInSql(sparkSession, sql);

        // then
        assertEquals(Set.of("table1", "table2"), set);

    }

}