package dataengine.spark.sql.logicalplan.functionresolver;

import dataengine.scala.compat.*;
import dataengine.spark.sql.udf.Udf;
import lombok.Value;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import scala.Option;
import scala.collection.JavaConverters;

import javax.annotation.Nonnull;
import java.util.Collections;

@Value
public class UnresolvedUdfReplacer implements UnresolvedFunctionReplacer {

    @Nonnull
    Udf udf;

    public Expression replace(@Nonnull UnresolvedFunction unresolvedFunction) throws FunctionResolverException {

        if (unresolvedFunction.children().size() != getArgumentsCount()) {
            throw new FunctionResolverException("arguments provided for function " + unresolvedFunction + " do not match udf expected number " + udf);
        }

        return new ScalaUDF(getScalaFunction(),
                udf.getReturnType(),
                unresolvedFunction.children(),
                JavaConverters.asScalaBuffer(Collections.emptyList()),
                Option.apply(unresolvedFunction.name().funcName()),
                true,
                true);

    }

    private JavaUdfToScalaFunction getScalaFunction() throws FunctionResolverException {
        if (udf.getUdf0() != null)
            return new JavaUdf0ToScalaFunction0<>(udf.getUdf0());
        if (udf.getUdf1() != null)
            return new JavaUdf1ToScalaFunction1<>(udf.getUdf1());
        if (udf.getUdf2() != null)
            return new JavaUdf2ToScalaFunction2<>(udf.getUdf2());
        if (udf.getUdf3() != null)
            return new JavaUdf3ToScalaFunction3<>(udf.getUdf3());
        if (udf.getUdf4() != null)
            return new JavaUdf4ToScalaFunction4<>(udf.getUdf4());
        if (udf.getUdf5() != null)
            return new JavaUdf5ToScalaFunction5<>(udf.getUdf5());
        throw new FunctionResolverException("no udf defined " + this);
    }

    private int getArgumentsCount() throws FunctionResolverException {
        if (udf.getUdf0() != null)
            return 0;
        if (udf.getUdf1() != null)
            return 1;
        if (udf.getUdf2() != null)
            return 2;
        if (udf.getUdf3() != null)
            return 3;
        if (udf.getUdf4() != null)
            return 4;
        if (udf.getUdf5() != null)
            return 5;
        throw new FunctionResolverException("no udf defined " + this);
    }

}
