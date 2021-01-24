package sparkengine.spark.sql.logicalplan.tablelist;

import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.Test;
import sparkengine.spark.sql.logicalplan.PlanExplorerException;
import sparkengine.spark.test.SparkSessionBase;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TableListExplorerTest extends SparkSessionBase {

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