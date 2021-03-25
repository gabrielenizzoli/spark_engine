package sparkengine.spark.sql.logicalplan.functionresolver;

import lombok.Value;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import scala.Option;
import scala.collection.JavaConverters;
import sparkengine.scala.compat.*;
import sparkengine.spark.sql.udf.context.GlobalUdfContext;
import sparkengine.spark.sql.udf.UdfDefinition;
import sparkengine.spark.sql.udf.context.UdfContext;
import sparkengine.spark.sql.udf.context.UdfWithContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;

@Value
public class UnresolvedUdfReplacer implements UnresolvedFunctionReplacer {

    @Nonnull
    UdfDefinition udfDefinition;
    @Nullable
    Broadcast<UdfContext> udfContext;

    public Expression replace(@Nonnull UnresolvedFunction unresolvedFunction) throws FunctionResolverException {

        if (unresolvedFunction.children().size() != getArgumentsCount()) {
            throw new FunctionResolverException("arguments provided for function " + unresolvedFunction + " do not match udf expected number " + udfDefinition);
        }

        return new ScalaUDF(getScalaFunction(),
                udfDefinition.getReturnType(),
                unresolvedFunction.children(),
                JavaConverters.asScalaBuffer(Collections.emptyList()),
                Option.apply(unresolvedFunction.name().funcName()),
                true,
                true);

    }

    private JavaUdfToScalaFunction getScalaFunction() throws FunctionResolverException {
        if (udfDefinition.asUdf0() != null)
            return new JavaUdf0ToScalaFunction0<>(injectUdfContext(udfDefinition.asUdf0()));
        if (udfDefinition.asUdf1() != null)
            return new JavaUdf1ToScalaFunction1<>(injectUdfContext(udfDefinition.asUdf1()));
        if (udfDefinition.asUdf2() != null)
            return new JavaUdf2ToScalaFunction2<>(injectUdfContext(udfDefinition.asUdf2()));
        if (udfDefinition.asUdf3() != null)
            return new JavaUdf3ToScalaFunction3<>(injectUdfContext(udfDefinition.asUdf3()));
        if (udfDefinition.asUdf4() != null)
            return new JavaUdf4ToScalaFunction4<>(injectUdfContext(udfDefinition.asUdf4()));
        if (udfDefinition.asUdf5() != null)
            return new JavaUdf5ToScalaFunction5<>(injectUdfContext(udfDefinition.asUdf5()));
        throw new FunctionResolverException("no udf defined " + this);
    }

    protected <T> T injectUdfContext(T udf) {
        if (udfContext != null && udf instanceof UdfWithContext)
            ((UdfWithContext)udf).setUdfContext(udfContext);
        return udf;
    }

    private int getArgumentsCount() throws FunctionResolverException {
        if (udfDefinition.asUdf0() != null)
            return 0;
        if (udfDefinition.asUdf1() != null)
            return 1;
        if (udfDefinition.asUdf2() != null)
            return 2;
        if (udfDefinition.asUdf3() != null)
            return 3;
        if (udfDefinition.asUdf4() != null)
            return 4;
        if (udfDefinition.asUdf5() != null)
            return 5;
        throw new FunctionResolverException("no udf defined " + this);
    }

}
