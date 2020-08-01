package dataengine.spark.sql;

import dataengine.scala.compat.JavaToScalaFunction1;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import scala.Option;
import scala.collection.JavaConverters;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

@Value
@Builder
public class FunctionResolver implements Function<LogicalPlan, LogicalPlan> {

    @Nonnull
    @Singular
    Map<String, UdfFactory> functionFactories;

    public class ExpressionMapper implements Function<Expression, Expression> {

        @Override
        public Expression apply(Expression expression) {
            if (expression instanceof UnresolvedFunction) {
                UnresolvedFunction unresolvedFunction = (UnresolvedFunction) expression;
                String name = unresolvedFunction.name().funcName();
                UdfFactory udfFactory = functionFactories.get(name);
                // TODO exception if missing
                expression = udfFactory.buildScalaUdf(unresolvedFunction);
            }

            return expression.mapChildren(new JavaToScalaFunction1<>(this));
        }

    }

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        logicalPlan = (LogicalPlan)logicalPlan.mapExpressions(new JavaToScalaFunction1<>(new ExpressionMapper()));
        return logicalPlan.mapChildren(new JavaToScalaFunction1<>(this));
    }

}
