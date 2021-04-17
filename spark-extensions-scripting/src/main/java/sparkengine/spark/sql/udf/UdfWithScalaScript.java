package sparkengine.spark.sql.udf;

import lombok.ToString;
import lombok.Value;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataType;
import scala.*;
import scala.tools.reflect.ToolBoxError;
import sparkengine.scala.scripting.ScriptEngine;
import sparkengine.spark.sql.udf.context.UdfContext;
import sparkengine.spark.sql.udf.context.UdfWithContext;

import javax.annotation.Nonnull;
import java.util.Objects;

@Value
public class UdfWithScalaScript implements UdfDefinition {

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
    public <R> UDF0<R> asUdf0() {
        if (numberOfInputParameters != 0)
            return null;
        return new ScriptUDF0<R>(code);
    }

    @Override
    public <I1, R> UDF1<I1, R> asUdf1() {
        if (numberOfInputParameters != 1)
            return null;
        return new ScriptUDF1<I1, R>(code);
    }

    @Override
    public <I1, I2, R> UDF2<I1, I2, R> asUdf2() {
        if (numberOfInputParameters != 2)
            return null;
        return new ScriptUDF2<I1, I2, R>(code);
    }

    @Override
    public <I1, I2, I3, R> UDF3<I1, I2, I3, R> asUdf3() {
        if (numberOfInputParameters != 3)
            return null;
        return new ScriptUDF3<I1, I2, I3, R>(code);
    }

    @Override
    public <I1, I2, I3, I4, R> UDF4<I1, I2, I3, I4, R> asUdf4() {
        if (numberOfInputParameters != 4)
            return null;
        return new ScriptUDF4<I1, I2, I3, I4, R>(code);
    }

    @Override
    public <I1, I2, I3, I4, I5, R> UDF5<I1, I2, I3, I4, I5, R> asUdf5() {
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

@ToString
class ScriptUDF0<R> implements UDF0<R>, UdfWithContext {

    private final String code;
    private Broadcast<UdfContext> udfContextBroadcast;

    public ScriptUDF0(@Nonnull String code) {
        this.code = Objects.requireNonNull(code);
    }

    public void setUdfContextBroadcast(@Nonnull Broadcast<UdfContext> udfContextBroadcast) {
        this.udfContextBroadcast = udfContextBroadcast;
    }

    @Override
    public R call() throws Exception {
        try {
            return (R) ((Function0<R>) ScriptEngine
                    .evaluate(code, true, udfContextBroadcast == null ? Option.empty() : Option.apply(udfContextBroadcast.getValue()), Option.apply(UdfContext.class.getName())))
                    .apply();
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}

@ToString
class ScriptUDF1<T1, R> implements UDF1<T1, R>, UdfWithContext {

    private final String code;
    private Broadcast<UdfContext> udfContext;

    public ScriptUDF1(@Nonnull String code) {
        this.code = Objects.requireNonNull(code);
    }

    @Override
    public void setUdfContextBroadcast(@Nonnull Broadcast<UdfContext> udfContextBroadcast) {
        this.udfContext = udfContextBroadcast;
    }

    @Override
    public R call(T1 o) throws Exception {
        try {
            return (R) ((Function1<T1, R>) ScriptEngine
                    .evaluate(code, true, udfContext == null ? Option.empty() : Option.apply(udfContext.getValue()), Option.apply(UdfContext.class.getName())))
                    .apply(o);
        } catch (ToolBoxError e) {
            throw new Exception(e);
        }
    }

}

@ToString
class ScriptUDF2<T1, T2, R> implements UDF2<T1, T2, R>, UdfWithContext {

    private final String code;
    private Broadcast<UdfContext> udfContext;

    public ScriptUDF2(@Nonnull String code) {
        this.code = Objects.requireNonNull(code);
    }

    @Override
    public void setUdfContextBroadcast(@Nonnull Broadcast<UdfContext> udfContextBroadcast) {
        this.udfContext = udfContextBroadcast;
    }

    @Override
    public R call(T1 o1, T2 o2) throws Exception {
        try {
            return (R) ((Function2<T1, T2, R>) ScriptEngine
                    .evaluate(code, true, udfContext == null ? Option.empty() : Option.apply(udfContext.getValue()), Option.apply(UdfContext.class.getName())))
                    .apply(o1, o2);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}

@ToString
class ScriptUDF3<T1, T2, T3, R> implements UDF3<T1, T2, T3, R>, UdfWithContext {

    private final String code;
    private Broadcast<UdfContext> udfContext;

    public ScriptUDF3(@Nonnull String code) {
        this.code = Objects.requireNonNull(code);
    }

    @Override
    public void setUdfContextBroadcast(@Nonnull Broadcast<UdfContext> udfContextBroadcast) {
        this.udfContext = udfContextBroadcast;
    }

    @Override
    public R call(T1 o1, T2 o2, T3 o3) throws Exception {
        try {
            return (R) ((Function3<T1, T2, T3, R>) ScriptEngine
                    .evaluate(code, true, udfContext == null ? Option.empty() : Option.apply(udfContext.getValue()), Option.apply(UdfContext.class.getName())))
                    .apply(o1, o2, o3);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}

@ToString
class ScriptUDF4<T1, T2, T3, T4, R> implements UDF4<T1, T2, T3, T4, R>, UdfWithContext {

    private final String code;
    private Broadcast<UdfContext> udfContext;

    public ScriptUDF4(@Nonnull String code) {
        this.code = Objects.requireNonNull(code);
    }

    @Override
    public void setUdfContextBroadcast(@Nonnull Broadcast<UdfContext> udfContextBroadcast) {
        this.udfContext = udfContextBroadcast;
    }

    @Override
    public R call(T1 o1, T2 o2, T3 o3, T4 o4) throws Exception {
        try {
            return (R) ((Function4<T1, T2, T3, T4, R>) ScriptEngine
                    .evaluate(code, true, udfContext == null ? Option.empty() : Option.apply(udfContext.getValue()), Option.apply(UdfContext.class.getName())))
                    .apply(o1, o2, o3, o4);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}

@ToString
class ScriptUDF5<T1, T2, T3, T4, T5, R> implements UDF5<T1, T2, T3, T4, T5, R>, UdfWithContext {

    private final String code;
    private Broadcast<UdfContext> udfContext;

    public ScriptUDF5(@Nonnull String code) {
        this.code = Objects.requireNonNull(code);
    }

    @Override
    public void setUdfContextBroadcast(@Nonnull Broadcast<UdfContext> udfContextBroadcast) {
        this.udfContext = udfContextBroadcast;
    }

    @Override
    public R call(T1 o1, T2 o2, T3 o3, T4 o4, T5 o5) throws Exception {
        try {
            return (R) ((Function5<T1, T2, T3, T4, T5, R>) ScriptEngine
                    .evaluate(code, true, udfContext == null ? Option.empty() : Option.apply(udfContext.getValue()), Option.apply(UdfContext.class.getName())))
                    .apply(o1, o2, o3, o4, o5);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

}
