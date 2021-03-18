package sparkengine.spark.sql.udf;

import org.apache.spark.sql.expressions.SparkUserDefinedFunction;
import scala.*;
import scala.tools.reflect.ToolBoxError;
import sparkengine.scala.scripting.ScriptEngine;

public class ScalaUdfCompiler {

    public static Udf compile(String name, String code) throws UdfCompilationException {
        try {
            var udf = ScriptEngine.compileToUserDefinedFunction(code);
            var sparkUdf = (SparkUserDefinedFunction) udf;
            var returnType = sparkUdf.dataType();
            int numberOfInputParameters = getNumberOfInputParameters(sparkUdf);
            return new UdfWithScalaScript(name, code, numberOfInputParameters, returnType);
        } catch (ToolBoxError e) {
            throw new UdfCompilationException(String.format("scala code does not compile [%s]", code), e);
        } catch (ClassCastException e) {
            throw new UdfCompilationException("scala code does not compile to a spark udf", e);
        } catch (IllegalArgumentException e) {
            throw new UdfCompilationException("scala code does not compile to a spark udf with proper signature", e);
        }
    }

    private static int getNumberOfInputParameters(SparkUserDefinedFunction sparkUdf) throws IllegalArgumentException {
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
        throw new IllegalArgumentException(String.format("udf function [%s] provided is not a function or it has a number of arguments that is not managed", sparkUdf.f().getClass().getSimpleName()));
    }

}
