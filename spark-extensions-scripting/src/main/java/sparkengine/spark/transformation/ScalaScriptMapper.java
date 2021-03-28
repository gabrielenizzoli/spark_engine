package sparkengine.spark.transformation;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Function1;
import scala.Option;
import scala.tools.reflect.ToolBoxError;
import sparkengine.scala.scripting.ScriptEngine;
import sparkengine.spark.transformation.context.TransformationContext;
import sparkengine.spark.transformation.context.TransformationWithContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class ScalaScriptMapper<T, U> implements TransformationWithContext, MapFunction<T, U> {

    @Nonnull
    private final String code;
    @Nullable
    private Broadcast<TransformationContext> transformationContextBroadcast;

    public ScalaScriptMapper(@Nonnull String code) {
        this.code = Objects.requireNonNull(code);
    }

    @Override
    public U call(T input) throws Exception {
        try {
            var mapper = getScalaScriptMapFunction();
            return mapper.apply(input);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

    @Override
    public void setTransformationContext(@Nonnull Broadcast<TransformationContext> transformationContextBroadcast) {
        this.transformationContextBroadcast = transformationContextBroadcast;
    }

    private Function1<T, U> getScalaScriptMapFunction() throws ToolBoxError {
        var ctx = Option.<Object>apply(transformationContextBroadcast == null ? TransformationContext.EMPTY_UDF_CONTEXT : transformationContextBroadcast.getValue());
        return ((Function1<T, U>) ScriptEngine.evaluate(code, false, ctx, Option.apply(TransformationContext.class.getName())));
    }

}
