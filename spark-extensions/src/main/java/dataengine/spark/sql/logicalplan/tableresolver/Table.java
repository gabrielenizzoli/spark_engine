package dataengine.spark.sql.logicalplan.tableresolver;

import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@Value(staticConstructor = "of")
public class Table {
    String name;
    LogicalPlan logicalPlan;

    public static <T> Table ofDataset(String name, Dataset<T> ds) {
        return of(name, ds.logicalPlan());
    }

}
