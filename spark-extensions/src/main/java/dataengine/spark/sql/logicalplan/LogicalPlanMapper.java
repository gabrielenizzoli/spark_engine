package dataengine.spark.sql.logicalplan;

import dataengine.scala.compat.JavaToScalaFunction1;
import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.function.Function;

/**
 * Maps a logical input plan to something different.
 * The specific logic is obviously on the implementation.
 */
@FunctionalInterface
public interface LogicalPlanMapper {

    /**
     * Maps an input logical plan in an output plan. The plan still needs to be compiled.
     *
     * @param logicalPlan The input logical plan
     * @return The transformed input plan
     * @throws PlanMapperException In case of abnormal input or operation, an exception may be thrown.
     */
    LogicalPlan map(LogicalPlan logicalPlan) throws PlanMapperException;

    default LogicalPlan mapChildrenOfLogicalPlan(LogicalPlan logicalPlan) {
        return logicalPlan.mapChildren(asScalaFunction());
    }

    /**
     * Utility that provides this mapper as a scala function (to be used with scala-specific apis).
     * @return A scala function that implements this mapper
     */
    default JavaToScalaFunction1<LogicalPlan, LogicalPlan> asScalaFunction() {

        Function<LogicalPlan, LogicalPlan> javaFunction = new Function<LogicalPlan, LogicalPlan>() {
            @Override
            @SneakyThrows(PlanMapperException.class)
            public LogicalPlan apply(LogicalPlan logicalPlan) {
                return map(logicalPlan);
            }
        };

        return new JavaToScalaFunction1<>(javaFunction);
    }

}
