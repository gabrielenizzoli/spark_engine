package sparkengine.spark.sql.logicalplan.tableresolver;

import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.annotation.Nonnull;

@Value(staticConstructor = "of")
public class Table {

    @Nonnull
    String name;
    @Nonnull
    LogicalPlan logicalPlan;

    public static <T> Table ofDataset(String name, Dataset<T> ds) {
        return of(name, ds.logicalPlan());
    }

}
