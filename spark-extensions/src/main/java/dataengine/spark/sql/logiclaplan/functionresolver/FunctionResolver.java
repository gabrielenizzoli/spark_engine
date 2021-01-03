package dataengine.spark.sql.logiclaplan.functionresolver;

import dataengine.spark.sql.logicalplan.ExpressionMapper;
import dataengine.spark.sql.logicalplan.LogicalPlanMapper;
import dataengine.spark.sql.logicalplan.PlanMapperException;
import dataengine.spark.sql.udf.SqlFunction;
import dataengine.spark.sql.udf.Udaf;
import dataengine.spark.sql.udf.Udf;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@Value
@Builder(builderClassName = "Builder")
public class FunctionResolver implements LogicalPlanMapper {

    @Nonnull
    @Singular
    Map<String, UnresolvedFunctionReplacer> functionReplacers;

    public static class Builder {

        public Builder udf(@Nullable Udf udf) {
            if (udf == null)
                return this;
            return functionReplacer(udf.getName(), new UnresolvedUdfReplacer(udf));
        }

        public Builder udaf(@Nullable Udaf udaf) {
            if (udaf == null)
                return this;
            return functionReplacer(udaf.getName(), new UnresolvedUdafReplacer(udaf));
        }

        public Builder sqlFunction(@Nullable SqlFunction sqlFunction) {
            if (sqlFunction == null)
                return this;
            if (sqlFunction instanceof Udf)
                return udf((Udf) sqlFunction);
            if (sqlFunction instanceof Udaf)
                return udaf((Udaf) sqlFunction);
            throw new IllegalArgumentException("Sql function provided in not a Udf or a Udaf function: " + sqlFunction.getClass().getName());
        }

        public Builder sqlFunctions(SqlFunction... sqlFunctions) {
            if (sqlFunctions != null)
                Arrays.stream(sqlFunctions).forEach(this::sqlFunction);
            return this;
        }

        public Builder sqlFunctions(@Nullable Collection<SqlFunction> sqlFunctionCollection) {
            if (sqlFunctionCollection != null)
                sqlFunctionCollection.forEach(this::sqlFunction);
            return this;
        }

    }

    public static FunctionResolver with(SqlFunction... sqlFunctions) {
        return builder().sqlFunctions(sqlFunctions).build();
    }

    @Override
    public LogicalPlan map(LogicalPlan logicalPlan) throws PlanMapperException {
        var logicalPlanWithFunctionsResolved = UnresolvedFunctionMapper.of(this, functionReplacers).mapExpressionsInsideLogicalPlan(logicalPlan);
        return mapChildrenOfLogicalPlan(logicalPlanWithFunctionsResolved);
    }

    @Value(staticConstructor = "of")
    private static class UnresolvedFunctionMapper implements ExpressionMapper {

        @Nonnull
        FunctionResolver functionResolver;
        @Nonnull
        Map<String, UnresolvedFunctionReplacer> functionReplacers;

        @SuppressWarnings("unchecked")
        public Expression map(Expression expression) throws PlanMapperException {

            if (expression instanceof UnresolvedFunction) {
                expression = replaceFunction((UnresolvedFunction) expression);
            } else if (expression instanceof PlanExpression) {
                var subQuery = (PlanExpression<LogicalPlan>) expression;
                var subQueryWithFunctionsResolved = functionResolver.map(subQuery.plan());
                expression = subQuery.withNewPlan(subQueryWithFunctionsResolved);
            }

            return mapChildrenOfExpression(expression);
        }

        private Expression replaceFunction(@Nonnull UnresolvedFunction unresolvedFunction) throws FunctionResolverException {
            var functionName = unresolvedFunction.name().funcName();
            var unresolvedFunctionResolver = functionReplacers.get(functionName);

            // if replacer found, prioritize it over a possible default function
            if (unresolvedFunctionResolver != null) {
                return unresolvedFunctionResolver.replace(unresolvedFunction);
            }

            // if default function found, let the unresolved function go, as it will be replaced later
            if (FunctionRegistry.expressions().keySet().contains(functionName)) {
                return unresolvedFunction;
            }

            throw new FunctionResolverException("can't resolve function " + functionName + " in unresolved function " + unresolvedFunction);
        }

    }

}
