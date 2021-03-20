package sparkengine.spark.sql.udf;

import lombok.Value;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataType;
import scala.*;
import scala.tools.reflect.ToolBoxError;
import sparkengine.scala.scripting.ScriptEngine;

import javax.annotation.Nonnull;

@Value
public class UdfWithScalaScript implements Udf {

    @Nonnull
    String name;
    @Nonnull
    String code;
    int numberOfInputParameters;
    @Nonnull
    DataType dataType;

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Override
    public <R> UDF0<R> getUdf0() {
        if (numberOfInputParameters != 0)
            return null;
        return new ScriptUDF0<R>(code);
    }

    @Override
    public <I1, R> UDF1<I1, R> getUdf1() {
        if (numberOfInputParameters != 1)
            return null;
        return new ScriptUDF1<I1, R>(code);
    }

    @Override
    public <I1, I2, R> UDF2<I1, I2, R> getUdf2() {
        if (numberOfInputParameters != 2)
            return null;
        return new ScriptUDF2<I1, I2, R>(code);
    }

    @Override
    public <I1, I2, I3, R> UDF3<I1, I2, I3, R> getUdf3() {
        if (numberOfInputParameters != 3)
            return null;
        return new ScriptUDF3<I1, I2, I3, R>(code);
    }

    @Override
    public <I1, I2, I3, I4, R> UDF4<I1, I2, I3, I4, R> getUdf4() {
        if (numberOfInputParameters != 4)
            return null;
        return new ScriptUDF4<I1, I2, I3, I4, R>(code);
    }

    @Override
    public <I1, I2, I3, I4, I5, R> UDF5<I1, I2, I3, I4, I5, R> getUdf5() {
        if (numberOfInputParameters != 5)
            return null;
        return new ScriptUDF5<I1, I2, I3, I4, I5, R>(code);
    }

    @Nonnull
    @Override
    public DataType getReturnType() {
        return dataType;
    }

}

@Value
class ScriptUDF0<R> implements UDF0<R> {

    @Nonnull
    String code;

    @Override
    public R call() throws Exception {
        try {
            return (R) ((Function0<R>) ScriptEngine.evaluate(code, Option.empty())).apply();
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}

@Value
class ScriptUDF1<T1, R> implements UDF1<T1, R> {

    @Nonnull
    String code;

    @Override
    public R call(T1 o) throws Exception {
        try {
            return (R) ((Function1<T1, R>) ScriptEngine.evaluate(code, Option.empty())).apply(o);
        } catch (ToolBoxError e) {
            throw new Exception(e);
        }
    }

}

@Value
class ScriptUDF2<T1, T2, R> implements UDF2<T1, T2, R> {

    @Nonnull
    String code;

    @Override
    public R call(T1 o1, T2 o2) throws Exception {
        try {
            return (R) ((Function2<T1, T2, R>) ScriptEngine.evaluate(code, Option.empty())).apply(o1, o2);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}

@Value
class ScriptUDF3<T1, T2, T3, R> implements UDF3<T1, T2, T3, R> {

    @Nonnull
    String code;

    @Override
    public R call(T1 o1, T2 o2, T3 o3) throws Exception {
        try {
            return (R) ((Function3<T1, T2, T3, R>) ScriptEngine.evaluate(code, Option.empty())).apply(o1, o2, o3);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}

@Value
class ScriptUDF4<T1, T2, T3, T4, R> implements UDF4<T1, T2, T3, T4, R> {

    @Nonnull
    String code;

    @Override
    public R call(T1 o1, T2 o2, T3 o3, T4 o4) throws Exception {
        try {
            return (R) ((Function4<T1, T2, T3, T4, R>) ScriptEngine.evaluate(code, Option.empty())).apply(o1, o2, o3, o4);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}

@Value
class ScriptUDF5<T1, T2, T3, T4, T5, R> implements UDF5<T1, T2, T3, T4, T5, R> {

    @Nonnull
    String code;

    @Override
    public R call(T1 o1, T2 o2, T3 o3, T4 o4, T5 o5) throws Exception {
        try {
            return (R) ((Function5<T1, T2, T3, T4, T5, R>) ScriptEngine.evaluate(code, Option.empty())).apply(o1, o2, o3, o4, o5);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}
