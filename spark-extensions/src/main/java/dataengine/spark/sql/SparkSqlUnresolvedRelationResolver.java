package dataengine.spark.sql;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.compat.java8.functionConverterImpls.FromJavaFunction;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.function.Function;

@Value
@Builder
public class SparkSqlUnresolvedRelationResolver {

    @Singular
    @Nonnull
    Map<String, LogicalPlan> plans;

    public class UnresolvedRelationResolver implements Function<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan logicalPlan) {
            if (!(logicalPlan instanceof UnresolvedRelation))
                return logicalPlan.mapChildren(new FromJavaFunction<>(this));
            LogicalPlan resolvedRelation = plans.get(((UnresolvedRelation) logicalPlan).tableName());
            if (resolvedRelation == null)
                return logicalPlan.mapChildren(new FromJavaFunction<>(this));
            return resolvedRelation;
        }

    }

    public LogicalPlan resolve(SparkSession sparkSession, String sql) {
        LogicalPlan logicalPlanWithUnresolvedRelations = null;
        try {
            logicalPlanWithUnresolvedRelations = sparkSession.sessionState().sqlParser().parsePlan(sql);
        } catch (ParseException e) {
            throw new IllegalArgumentException("bad sql", e);
        }
        return logicalPlanWithUnresolvedRelations.mapChildren(new FromJavaFunction<>(new UnresolvedRelationResolver()));
    }

    public Dataset<Row> resolveAsDataset(SparkSession sparkSession, String sql) {
        return Dataset.ofRows(sparkSession, resolve(sparkSession, sql));
    }

}
