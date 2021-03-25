package sparkengine.spark.sql.logicalplan.functionresolver;

import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.PlanExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import sparkengine.spark.sql.logicalplan.ExpressionMapper;
import sparkengine.spark.sql.logicalplan.LogicalPlanMapper;
import sparkengine.spark.sql.logicalplan.PlanMapperException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Value
public class FunctionResolver implements LogicalPlanMapper {

    @Nonnull
    @Singular
    Map<String, UnresolvedFunctionReplacer> functionReplacers;

    public static FunctionResolver of(@Nullable Function... functions) {
        Map<String, UnresolvedFunctionReplacer> functionReplacers = new HashMap<>();
        if (functions != null)
            Arrays.stream(functions).forEach(f -> f.addToMap(functionReplacers));
        return new FunctionResolver(functionReplacers);
    }

    public static FunctionResolver of(@Nullable Collection<Function> functions) {
        Map<String, UnresolvedFunctionReplacer> functionReplacers = new HashMap<>();
        if (functions != null)
            functions.forEach(f -> f.addToMap(functionReplacers));
        return new FunctionResolver(functionReplacers);
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
