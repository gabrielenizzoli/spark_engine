package sparkengine.spark.transformation;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Function1;
import scala.Option;
import scala.tools.reflect.ToolBoxError;
import sparkengine.scala.scripting.ScriptEngine;
import sparkengine.spark.transformation.context.TransformationContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
public class ScalaScriptMapper<T, U> implements MapFunction<T, U> {

    @Nonnull
    String code;
    @Nullable
    Broadcast<TransformationContext> transformationContextBroadcast;

    @Override
    public U call(T input) throws Exception {
        try {
            var mapper = getScalaScriptMapFunction();
            return mapper.apply(input);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

    private Function1<T, U> getScalaScriptMapFunction() throws ToolBoxError {
        var ctx = Option.<Object>apply(transformationContextBroadcast == null ? TransformationContext.EMPTY_UDF_CONTEXT : transformationContextBroadcast.getValue());
        return ((Function1<T, U>) ScriptEngine.evaluate(code, false, ctx, Option.apply(TransformationContext.class.getName())));
    }

}
