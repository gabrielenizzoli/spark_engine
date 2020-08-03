package dataengine.spark.sql;

import dataengine.scala.compat.JavaToScalaFunction1;
import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.function.Function;

@FunctionalInterface
public interface LogicalPlanMapper {

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
