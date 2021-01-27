package sparkengine.spark.sql.logicalplan;

import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import sparkengine.scala.compat.JavaToScalaFunction1;

import java.util.function.Function;

public interface ExpressionExplorer {

    void exploreExpression(Expression expression) throws PlanExplorerException;

    default void exploreChildrenOfExpression(Expression expression) {
        expression.mapChildren(asScalaFunction());
    }

    default void exploreExpressionsInsideLogicalPlan(LogicalPlan logicalPlan) {
        logicalPlan.mapExpressions(asScalaFunction());
    }

    default JavaToScalaFunction1<Expression, Expression> asScalaFunction() {

        Function<Expression, Expression> javaFunction = new Function<Expression, Expression>() {
            @Override
            @SneakyThrows(PlanExplorerException.class)
            public Expression apply(Expression expression) {
                exploreExpression(expression);
                return expression;
            }
        };

        return new JavaToScalaFunction1<>(javaFunction);
    }

}
