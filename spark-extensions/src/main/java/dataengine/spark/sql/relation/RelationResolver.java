package dataengine.spark.sql.relation;

import dataengine.spark.sql.ExpressionMapper;
import dataengine.spark.sql.LogicalPlanMapper;
import dataengine.spark.sql.PlanMapperException;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.PlanExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder
public class RelationResolver implements LogicalPlanMapper {

    @Singular
    @Nonnull
    Map<String, LogicalPlan> plans;

    public class PlanExpressionMapper implements ExpressionMapper {

        @SuppressWarnings("unchecked")
        public Expression map(Expression expression) throws PlanMapperException {

             if (expression instanceof PlanExpression) {
                PlanExpression<LogicalPlan> subquery = (PlanExpression<LogicalPlan>) expression;
                expression = subquery.withNewPlan(RelationResolver.this.map(subquery.plan()));
            }

            return expression.mapChildren(asScalaFunction());
        }

    }

    @Override
    public LogicalPlan map(LogicalPlan logicalPlan) throws PlanMapperException {

        if (logicalPlan instanceof UnresolvedRelation) {
            UnresolvedRelation unresolvedRelation = (UnresolvedRelation) logicalPlan;
            String name = unresolvedRelation.tableName();
            LogicalPlan resolvedRelation = plans.get(name);
            if (resolvedRelation == null) {
                throw new PlanMapperException("can't resolve relation " + name + " in plan " + unresolvedRelation);
            }
            logicalPlan = resolvedRelation;
        } else {
            logicalPlan = (LogicalPlan)logicalPlan.mapExpressions(new PlanExpressionMapper().asScalaFunction());
            logicalPlan = logicalPlan.mapChildren(asScalaFunction());
        }

        return logicalPlan;
    }

}
