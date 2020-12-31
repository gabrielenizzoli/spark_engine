package dataengine.spark.sql;

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

@Value
@Builder
public class SparkSqlPlanMapper {

    @Singular
    @Nonnull
    List<LogicalPlanMapper> planMappers;

    public Dataset<Row> compileSqlToDataset(@Nonnull SparkSession sparkSession,
                                            @Nonnull String sql) throws PlanMapperException {
        return Dataset.ofRows(sparkSession, compileSqlToLogicalPlan(sparkSession, sql));
    }

    private LogicalPlan compileSqlToLogicalPlan(@Nonnull SparkSession sparkSession,
                                                @Nonnull String sql) throws PlanMapperException {
        LogicalPlan logicalPlan = generateLogicalPlan(sparkSession, sql);
        logicalPlan = remapLogicalPlan(logicalPlan, planMappers);
        return logicalPlan;
    }

    private static LogicalPlan generateLogicalPlan(@Nonnull SparkSession sparkSession,
                                                   @Nonnull String sql) throws PlanMapperException {
        try {
            return sparkSession.sessionState().sqlParser().parsePlan(sql);
        } catch (ParseException e) {
            throw  new PlanMapperException("can't parse sql: " + sql, e);
        }
    }

    @Nonnull
    private static LogicalPlan remapLogicalPlan(@Nonnull final LogicalPlan logicalPlan,
                                                @Nonnull final List<LogicalPlanMapper> planMappers) throws PlanMapperException {
        LogicalPlan remappedLogicalPlan = logicalPlan;
        for (LogicalPlanMapper planMapper : planMappers) {
            remappedLogicalPlan = planMapper.map(remappedLogicalPlan);
        }
        return remappedLogicalPlan;
    }

}
