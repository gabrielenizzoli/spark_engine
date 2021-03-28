package sparkengine.spark.transformation;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConverters;
import scala.tools.reflect.ToolBoxError;
import sparkengine.scala.scripting.ScriptEngine;
import sparkengine.spark.transformation.context.TransformationContext;
import sparkengine.spark.transformation.context.TransformationWithContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;

public class ScalaScriptPartitionMapper<T, U> implements TransformationWithContext, MapPartitionsFunction<T, U> {

    @Nonnull
    private final String code;
    @Nullable
    private Broadcast<TransformationContext> transformationContextBroadcast;

    public ScalaScriptPartitionMapper(@Nonnull String code) {
        this.code = Objects.requireNonNull(code);
    }

    @Override
    public Iterator<U> call(Iterator<T> input) throws Exception {
        try {
            var mapper = getScalaScriptMapPartitionsFunction();
            var scalaInput = JavaConverters.asScalaIterator(input);
            var scalaOutput = mapper.apply(scalaInput);
            return JavaConverters.asJavaIterator(scalaOutput);
        } catch (ToolBoxError e) {
            throw new Exception("compilation error in scala code", e);
        }
    }

    @Override
    public void setTransformationContext(@Nonnull Broadcast<TransformationContext> transformationContextBroadcast) {
        this.transformationContextBroadcast = transformationContextBroadcast;
    }

    private Function1<scala.collection.Iterator<T>, scala.collection.Iterator<U>> getScalaScriptMapPartitionsFunction() throws ToolBoxError {
        var ctx = Option.<Object>apply(transformationContextBroadcast == null ? TransformationContext.EMPTY_UDF_CONTEXT : transformationContextBroadcast.getValue());
        return ((Function1<scala.collection.Iterator<T>, scala.collection.Iterator<U>>) ScriptEngine.evaluate(code, false, ctx, Option.apply(TransformationContext.class.getName())));
    }

}
