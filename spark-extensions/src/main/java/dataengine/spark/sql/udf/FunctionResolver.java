package dataengine.spark.sql.udf;

import dataengine.spark.sql.ExpressionMapper;
import dataengine.spark.sql.LogicalPlanMapper;
import dataengine.spark.sql.PlanMapperException;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.PlanExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

@Value
@Builder
public class FunctionResolver implements LogicalPlanMapper {

    @Nonnull
    @Singular
    Map<String, ExpressionResolver> expressionResolvers;

    public static class FunctionResolverBuilder {

        public FunctionResolverBuilder udf(@Nullable Udf udf) {
            if (udf == null)
                return this;
            return expressionResolver(udf.getName(), new UdfExpressionResolver(udf));
        }

        public FunctionResolverBuilder udaf(@Nullable Udaf udaf) {
            if (udaf == null)
                return this;
            return expressionResolver(udaf.getName(), new UdafExpressionResolver(udaf));
        }

        public FunctionResolverBuilder sqlFunction(@Nullable SqlFunction sqlFunction) {
            if (sqlFunction instanceof Udf)
                return udf((Udf)sqlFunction);
            if (sqlFunction instanceof Udaf)
                return udaf((Udaf)sqlFunction);
            // TODO manage exception
            return this;
        }

        public FunctionResolverBuilder sqlFunctions(@Nullable Collection<SqlFunction> sqlFunctionCollection) {
            if (sqlFunctionCollection == null)
                return this;
            sqlFunctionCollection.forEach(this::sqlFunction);
            return this;
        }

        public FunctionResolverBuilder sqlFunctions(@Nullable SqlFunctionCollection sqlFunctionCollection) {
            if (sqlFunctionCollection == null)
                return this;
            return sqlFunctions(sqlFunctionCollection.getSqlFunctions());
        }

    }

    public class UnresolvedFunctionMapper implements ExpressionMapper {

        @SuppressWarnings("unchecked")
        public Expression map(Expression expression) throws PlanMapperException {

            if (expression instanceof UnresolvedFunction) {
                UnresolvedFunction unresolvedFunction = (UnresolvedFunction) expression;
                String name = unresolvedFunction.name().funcName();
                ExpressionResolver expressionResolver = expressionResolvers.get(name);
                if (expressionResolver != null) {
                    expression = expressionResolver.resolve(unresolvedFunction);
                } else if (!FunctionRegistry.expressions().keySet().contains(name)) {
                    throw new FunctionResolverException("can't resolve function " + name + " in expression " + expression);
                }
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
