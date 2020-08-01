package dataengine.spark.sql;

import dataengine.scala.compat.JavaUdf0ToScalaFunction0;
import dataengine.scala.compat.JavaUdf1ToScalaFunction1;
import dataengine.scala.compat.JavaUdf2ToScalaFunction2;
import dataengine.scala.compat.JavaUdfToScalaFunction;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.types.DataType;
import scala.Option;
import scala.collection.JavaConverters;

import javax.annotation.Nonnull;
import java.util.Collections;

@Value
@Builder
public class UdfFactory {

    @Nonnull
    JavaUdfToScalaFunction userDefinedFunction;
    int argumentsCount;
    @Nonnull
    DataType returnType;
    @Builder.Default
    boolean returnValueNullable = true;
    @Builder.Default
    boolean deterministicFunction = true;

    public static class UdfFactoryBuilder {

        public UdfFactoryBuilder userDefinedFunction(UDF0<?> udf) {
            userDefinedFunction = new JavaUdf0ToScalaFunction0<>(udf);
            this.argumentsCount = 0;
            return this;
        }

        public UdfFactoryBuilder userDefinedFunction(UDF1<?, ?> udf) {
            userDefinedFunction = new JavaUdf1ToScalaFunction1<>(udf);
            this.argumentsCount = 1;
            return this;
        }

        public UdfFactoryBuilder userDefinedFunction(UDF2<?, ?, ?> udf) {
            userDefinedFunction = new JavaUdf2ToScalaFunction2<>(udf);
            this.argumentsCount = 2;
            return this;
        }

    }

    public ScalaUDF buildScalaUdf(UnresolvedFunction unresolvedFunction) {
        if (unresolvedFunction.children().size() != argumentsCount) {
            // TODO error
        }
        return new ScalaUDF(userDefinedFunction,
                returnType,
                unresolvedFunction.children(),
                JavaConverters.asScalaBuffer(Collections.emptyList()),
                Option.apply(unresolvedFunction.name().funcName()),
                returnValueNullable,
                deterministicFunction);
    }
}
