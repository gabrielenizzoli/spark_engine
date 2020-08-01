package dataengine.spark.sql;

import dataengine.scala.compat.JavaToScalaFunction1;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;

@Value
@Builder
public class SparkSqlPlanResolver {

    @Singular
    @Nonnull
    List<Function<LogicalPlan, LogicalPlan>> planMappers;

    private LogicalPlan getLogicalPlanForSql(SparkSession sparkSession, String sql) {
        LogicalPlan logicalPlanWithUnresolvedRelations = null;
        try {
            logicalPlanWithUnresolvedRelations = sparkSession.sessionState().sqlParser().parsePlan(sql);
        } catch (ParseException e) {
            throw new IllegalArgumentException("bad sql", e);
        }
        return logicalPlanWithUnresolvedRelations;
    }

    public Dataset<Row> resolveAsDataset(SparkSession sparkSession, String sql) {
        return Dataset.ofRows(sparkSession, resolveAsLogicalPlan(sparkSession, sql));
    }

    @SuppressWarnings("unchecked")
    private LogicalPlan resolveAsLogicalPlan(SparkSession sparkSession, String sql) {
        LogicalPlan logicalPlan = getLogicalPlanForSql(sparkSession, sql);
        for (Function<LogicalPlan, LogicalPlan> planMapper : planMappers) {
            logicalPlan = planMapper.apply(logicalPlan).mapChildren(new JavaToScalaFunction1<>(planMapper));
        }
        return logicalPlan;
    }

}
