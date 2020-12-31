package dataengine.spark.sql;

import dataengine.scala.compat.JavaToScalaFunction1;
import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.function.Function;

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
