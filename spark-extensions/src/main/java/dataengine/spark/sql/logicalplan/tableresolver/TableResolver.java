package dataengine.spark.sql.logicalplan.tableresolver;

import dataengine.spark.sql.logicalplan.ExpressionMapper;
import dataengine.spark.sql.logicalplan.LogicalPlanMapper;
import dataengine.spark.sql.logicalplan.PlanMapperException;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.PlanExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@Value
@Builder
public class TableResolver implements LogicalPlanMapper {

    @Singular
    @Nonnull
    Map<String, LogicalPlan> plans;

    public static class Builder {

        public Builder table(@Nullable Table table) {
            if (table != null)
                plan(table.getName(), table.getLogicalPlan());
            return this;
        }

        public Builder tables(Table... tables) {
            if (tables != null)
                Arrays.stream(tables).forEach(this::table);
            return this;
        }

        public Builder tables(@Nullable Collection<Table> tables) {
            if (tables != null)
                tables.forEach(this::table);
            return this;
        }

    }

    public static TableResolver with(Table... tables) {
        return builder().tables(tables).build();
    }

    @Override
    public LogicalPlan map(LogicalPlan logicalPlan) throws PlanMapperException {

        // NOTE: UnresolvedRelation do not have children, so no need to dig deeper
        if (logicalPlan instanceof UnresolvedRelation) {
            return replaceUnresolvedRelation((UnresolvedRelation) logicalPlan);
        }

        var logicalPlanWithExpressionsMapped = TableResolverInsideExpressions.of(this).mapExpressionsInsideLogicalPlan(logicalPlan);
        return mapChildrenOfLogicalPlan(logicalPlanWithExpressionsMapped);
    }

    @Nonnull
    private LogicalPlan replaceUnresolvedRelation(UnresolvedRelation unresolvedRelation) throws TableResolverException {
        var resolvedRelation = plans.get(unresolvedRelation.tableName());
        if (resolvedRelation == null) {
            throw new TableResolverException("can't resolve relation " + unresolvedRelation.tableName() + " in plan " + unresolvedRelation);
        }
        return resolvedRelation;
    }

    private LogicalPlan resolveUnresolvedExpressions(LogicalPlan logicalPlan) {
        return new TableResolverInsideExpressions(this).mapExpressionsInsideLogicalPlan(logicalPlan);
    }

    private LogicalPlan resolvedUnresolvedChildren(LogicalPlan logicalPlan) {
        return mapChildrenOfLogicalPlan(logicalPlan);
    }

    @Value(staticConstructor = "of")
    private static class TableResolverInsideExpressions implements ExpressionMapper {

        @Nonnull
        TableResolver tableResolver;

        @SuppressWarnings("unchecked")
        public Expression map(Expression expression) throws PlanMapperException {

            if (expression instanceof PlanExpression) {
                var subQuery = (PlanExpression<LogicalPlan>) expression;
                expression = subQuery.withNewPlan(tableResolver.map(subQuery.plan()));
            }

            return mapChildrenOfExpression(expression);
        }

    }

}
