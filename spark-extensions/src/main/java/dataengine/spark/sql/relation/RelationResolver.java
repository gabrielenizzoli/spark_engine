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

    @Override
    public LogicalPlan map(LogicalPlan logicalPlan) throws PlanMapperException {

        if (logicalPlan instanceof UnresolvedRelation) {
            logicalPlan = resolveUnresolvedRelation(logicalPlan);
        } else {
            logicalPlan = resolveUnresolvedExpressions(logicalPlan);
            logicalPlan = resolvedUnresolvedChildren(logicalPlan);
        }

        return logicalPlan;
    }

    @Nonnull
    private LogicalPlan resolveUnresolvedRelation(LogicalPlan logicalPlan) throws RelationResolverException {
        UnresolvedRelation unresolvedRelation = (UnresolvedRelation) logicalPlan;
        String name = unresolvedRelation.tableName();
        LogicalPlan resolvedRelation = plans.get(name);
        if (resolvedRelation == null) {
            throw new RelationResolverException("can't resolve relation " + name + " in plan " + unresolvedRelation);
        }
        return resolvedRelation;
    }

    private LogicalPlan resolveUnresolvedExpressions(LogicalPlan logicalPlan) {
        return  (LogicalPlan) logicalPlan.mapExpressions(new PlanExpressionMapper(this).asScalaFunction());
    }

    private LogicalPlan resolvedUnresolvedChildren(LogicalPlan logicalPlan) {
        return logicalPlan.mapChildren(asScalaFunction());
    }

    private static class PlanExpressionMapper implements ExpressionMapper {

        private final RelationResolver relationResolver;

        public PlanExpressionMapper(RelationResolver relationResolver) {
            this.relationResolver = relationResolver;
        }

        @SuppressWarnings("unchecked")
        public Expression map(Expression expression) throws PlanMapperException {

            if (expression instanceof PlanExpression) {
                PlanExpression<LogicalPlan> subquery = (PlanExpression<LogicalPlan>) expression;
                expression = subquery.withNewPlan(relationResolver.map(subquery.plan()));
            }

            return expression.mapChildren(asScalaFunction());
        }

    }

}
