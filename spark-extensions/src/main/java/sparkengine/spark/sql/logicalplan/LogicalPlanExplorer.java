package sparkengine.spark.sql.logicalplan;

import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import sparkengine.scala.compat.JavaToScalaFunction1;

import java.util.function.Function;

public interface LogicalPlanExplorer {

    void exploreLogicalPlan(LogicalPlan logicalPlan) throws PlanExplorerException;

    default void exploreChildrenOfLogicalPlan(LogicalPlan logicalPlan) {
        logicalPlan.mapChildren(asScalaFunction());
    }

    default JavaToScalaFunction1<LogicalPlan, LogicalPlan> asScalaFunction() {

        Function<LogicalPlan, LogicalPlan> javaFunction = new Function<LogicalPlan, LogicalPlan>() {
            @Override
            @SneakyThrows(PlanExplorerException.class)
            public LogicalPlan apply(LogicalPlan logicalPlan) {
                exploreLogicalPlan(logicalPlan);
                return logicalPlan;
            }
        };

        return new JavaToScalaFunction1<>(javaFunction);
    }

}
