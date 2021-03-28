package sparkengine.plan.runtime.builder.dataset.supplier;

import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import sparkengine.plan.model.component.ComponentWithSingleInput;
import sparkengine.plan.model.component.impl.EncodeComponent;
import sparkengine.plan.model.component.impl.MapComponent;
import sparkengine.plan.model.component.impl.SchemaValidationComponent;
import sparkengine.plan.runtime.builder.RuntimeContext;
import sparkengine.plan.runtime.builder.dataset.utils.EncoderUtils;
import sparkengine.plan.runtime.builder.dataset.utils.TransformationUtils;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.transformation.DataTransformation;
import sparkengine.spark.transformation.Transformations;
import sparkengine.spark.transformation.context.TransformationWithContext;

import javax.annotation.Nonnull;

@Value
@Builder
public class DatasetSupplierForComponentWithSingleInput<T> implements DatasetSupplier<T> {

    @Nonnull
    RuntimeContext runtimeContext;
    @Nonnull
    ComponentWithSingleInput componentWithSingleInput;
    @Nonnull
    Dataset inputDataset;

    @Override
    public Dataset<T> getDataset() throws DatasetFactoryException {
        if (componentWithSingleInput instanceof SchemaValidationComponent) {
            var schemaComponent = (SchemaValidationComponent)componentWithSingleInput;
            var schema = StructType.fromDDL(schemaComponent.getSchema());
            return (Dataset<T>) Transformations.verifySchemaWith(schema).apply(inputDataset);
        } else if (componentWithSingleInput instanceof EncodeComponent) {
            var encodeComponent = (EncodeComponent) componentWithSingleInput;
            Encoder<?> encoder = EncoderUtils.buildEncoder(encodeComponent.getEncodedAs());
            return (Dataset<T>) Transformations.encodeAs(encoder).apply(inputDataset);
        } else if (componentWithSingleInput instanceof MapComponent) {
            var mapComponent = (MapComponent) componentWithSingleInput;
            return (Dataset<T>) getMapDataset(mapComponent);
        }

        return null;
    }

    private Dataset<T> getMapDataset(MapComponent mapComponent) throws DatasetFactoryException {
        var dataTransformation = TransformationUtils.<T>getDataTransformation(mapComponent.getTransformWith());
        TransformationUtils.injectTransformationParameters(dataTransformation, mapComponent.getParams());

        if (dataTransformation instanceof TransformationWithContext) {
            var txWithContext = (TransformationWithContext)dataTransformation;
            txWithContext.setTransformationContext(runtimeContext.buildBroadcastTransformationContext(mapComponent.getAccumulators()));
        }

        if (mapComponent.getEncodedAs() != null) {
            var encoder = (Encoder<T>) EncoderUtils.buildEncoder(mapComponent.getEncodedAs());
            dataTransformation = dataTransformation.andThenEncode(encoder);
        }

        return dataTransformation.apply(inputDataset);
    }

}
