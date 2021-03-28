package sparkengine.spark.transformation;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import sparkengine.spark.transformation.context.DataTransformationContext;
import sparkengine.spark.transformation.context.DataTransformationWithContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class ScalaScriptTransformation<S, D> implements
        DataTransformationWithParameters<ScalaScriptParameters>,
        DataTransformationWithContext,
        DataTransformationWithEncoder<D>,
        DataTransformation<S, D> {

    private ScalaScriptParameters scalaScriptParameters;
    private Broadcast<DataTransformationContext> transformationContextBroadcast;
    private Encoder<D> encoder;

    @Override
    public Dataset<D> apply(Dataset<S> dataset) {
        var map = new ScalaScriptMapFunction<S, D>(scalaScriptParameters.getCode(), transformationContextBroadcast);
        return dataset.map(map, Objects.requireNonNull(encoder, "encoder is null"));
    }

    @Nonnull
    @Override
    public Class<ScalaScriptParameters> getParametersType() {
        return ScalaScriptParameters.class;
    }

    @Override
    public void setParameters(@Nullable ScalaScriptParameters parameter) {
        this.scalaScriptParameters = parameter;
    }

    @Override
    public void setTransformationContext(Broadcast<DataTransformationContext> transformationContextBroadcast) {
        this.transformationContextBroadcast = transformationContextBroadcast;
    }

    @Override
    public void setEncoder(Encoder<D> encoder) {
        this.encoder = Objects.requireNonNull(encoder, "encoder is null");
    }

}
