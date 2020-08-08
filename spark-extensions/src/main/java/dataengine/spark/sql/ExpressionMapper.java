package dataengine.spark.sql;

import dataengine.scala.compat.JavaToScalaFunction1;
import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.function.Function;

@FunctionalInterface
public interface ExpressionMapper {

    Expression map(Expression expression) throws PlanMapperException;

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
