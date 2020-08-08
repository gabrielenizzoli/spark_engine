package dataengine.spark.sql.udf;

import dataengine.spark.sql.ExpressionMapper;
import dataengine.spark.sql.LogicalPlanMapper;
import dataengine.spark.sql.PlanMapperException;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.PlanExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder
public class FunctionResolver implements LogicalPlanMapper {

    @Nonnull
    @Singular
    Map<String, ExpressionResolver> expressionResolvers;

    public static class FunctionResolverBuilder {

        public FunctionResolverBuilder udf(Udf udf) {
            return expressionResolver(udf.getName(), new UdfExpressionResolver(udf));
        }

    }

    public class UnresolvedFunctionMapper implements ExpressionMapper {

        @SuppressWarnings("unchecked")
        public Expression map(Expression expression) throws PlanMapperException {

            if (expression instanceof UnresolvedFunction) {
                UnresolvedFunction unresolvedFunction = (UnresolvedFunction) expression;
                String name = unresolvedFunction.name().funcName();
                ExpressionResolver udfFactory = expressionResolvers.get(name);
                if (udfFactory == null) {
                    throw new PlanMapperException("can't resolve function " + name + " in expression " + expression);
                }
                expression = udfFactory.resolve(unresolvedFunction);
            } else if (expression instanceof PlanExpression) {
                PlanExpression<LogicalPlan> subquery = (PlanExpression<LogicalPlan>) expression;
                expression = subquery.withNewPlan(FunctionResolver.this.map(subquery.plan()));
            }

            return expression.mapChildren(asScalaFunction());
        }

    }

    @Override
    public LogicalPlan map(LogicalPlan logicalPlan) throws PlanMapperException {
        logicalPlan = (LogicalPlan) logicalPlan.mapExpressions(new UnresolvedFunctionMapper().asScalaFunction());
        return logicalPlan.mapChildren(asScalaFunction());
    }

    public interface ExpressionResolver {

        Expression resolve(UnresolvedFunction unresolvedFunction) throws PlanMapperException;

    }

}
