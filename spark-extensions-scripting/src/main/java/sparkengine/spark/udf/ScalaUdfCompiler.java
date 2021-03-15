package sparkengine.spark.udf;

import org.apache.spark.sql.expressions.SparkUserDefinedFunction;
import scala.*;
import sparkengine.scala.scripting.ScriptEngine;
import sparkengine.spark.sql.udf.Udf;

public class ScalaUdfCompiler {

    public static Udf compile(String name, String code) {
        // TODO: should throw exception here
        var udf = ScriptEngine.compileToUserDefinedFunction(code);
        var sparkUdf = (SparkUserDefinedFunction) udf;
        var returnType = sparkUdf.dataType();
        int numberOfInputParameters = getNumberOfInputParameters(sparkUdf);
        return new UdfWithScalaScript(name, code, numberOfInputParameters, returnType);
    }

    private static int getNumberOfInputParameters(SparkUserDefinedFunction sparkUdf) {
        if (sparkUdf.f() instanceof Function0)
            return 0;
        else if (sparkUdf.f() instanceof Function1)
            return 1;
        else if (sparkUdf.f() instanceof Function2)
            return 2;
        else if (sparkUdf.f() instanceof Function3)
            return 3;
        else if (sparkUdf.f() instanceof Function4)
            return 4;
        else if (sparkUdf.f() instanceof Function5)
            return 5;
        // TODO: fix this exception
        throw new IllegalArgumentException();
    }

}
