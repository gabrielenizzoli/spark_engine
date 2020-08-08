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

    private LogicalPlan getLogicalPlanForSql(SparkSession sparkSession, String sql) {
        LogicalPlan logicalPlanWithUnresolvedRelations = null;
        try {
            logicalPlanWithUnresolvedRelations = sparkSession.sessionState().sqlParser().parsePlan(sql);
        } catch (ParseException e) {
            throw new IllegalArgumentException("bad sql", e);
        }
        return logicalPlanWithUnresolvedRelations;
    }

    @SuppressWarnings("unchecked")
    private LogicalPlan mapAsLogicalPlan(SparkSession sparkSession, String sql) throws PlanMapperException {
        LogicalPlan logicalPlan = getLogicalPlanForSql(sparkSession, sql);
        for (LogicalPlanMapper planMapper : planMappers) {
            logicalPlan = planMapper.map(logicalPlan).mapChildren(planMapper.asScalaFunction());
        }
        return logicalPlan;
    }

    public Dataset<Row> mapAsDataset(SparkSession sparkSession, String sql) throws PlanMapperException {
        return Dataset.ofRows(sparkSession, mapAsLogicalPlan(sparkSession, sql));
    }

}
