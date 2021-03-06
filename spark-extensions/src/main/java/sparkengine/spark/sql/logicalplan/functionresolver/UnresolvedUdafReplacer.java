package sparkengine.spark.sql.logicalplan.functionresolver;

import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete$;
import org.apache.spark.sql.execution.aggregate.ScalaAggregator;
import scala.Option;
import sparkengine.spark.sql.udf.UdafDefinition;

import javax.annotation.Nonnull;

@Value
public class UnresolvedUdafReplacer implements UnresolvedFunctionReplacer {

    @Nonnull
    UdafDefinition udafDefinition;

    public Expression replace(@Nonnull UnresolvedFunction unresolvedFunction) throws FunctionResolverException {

        ScalaAggregator scalaAggregator = new ScalaAggregator(
                unresolvedFunction.children(),
                udafDefinition.getAggregator(),
                (ExpressionEncoder) udafDefinition.inputEncoder(),
                (ExpressionEncoder) udafDefinition.getAggregator().bufferEncoder(),
                true,
                true,
                0,
                0
        );

        return new AggregateExpression(
                scalaAggregator,
                Complete$.MODULE$,
                false,
                Option.empty(),
                NamedExpression.newExprId()
        );

    }

}
