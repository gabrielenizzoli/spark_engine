package sparkengine.spark.sql.logicalplan;

import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import sparkengine.scala.compat.JavaToScalaFunction1;

import java.util.function.Function;

/**
 * Maps an input expression to something different.
 * The specific logic is dependent on the implementation.
 */
@FunctionalInterface
public interface ExpressionMapper {

    /**
     * Maps an input expression to an output one.
     *
     * @param expression Input expression
     * @return Transformed expression.
     * @throws PlanMapperException In case of abnormal input or operation, an exception may be thrown.
     */
    Expression map(Expression expression) throws PlanMapperException;

    default LogicalPlan mapExpressionsInsideLogicalPlan(LogicalPlan logicalPlan) {
        return (LogicalPlan) logicalPlan.mapExpressions(asScalaFunction());
    }

    default Expression mapChildrenOfExpression(Expression expression) {
        return expression.mapChildren(asScalaFunction());
    }

    /**
     * Utility that provides this mapper as a scala function (to be used with scala-specific apis).
     *
     * @return A scala function that implements this mapper
     */
    default JavaToScalaFunction1<Expression, Expression> asScalaFunction() {

        Function<Expression, Expression> javaFunction = new Function<Expression, Expression>() {
            @Override
            @SneakyThrows(PlanMapperException.class)
            public Expression apply(Expression expression) {
                return map(expression);
            }
        };

        return new JavaToScalaFunction1<>(javaFunction);
    }

}
