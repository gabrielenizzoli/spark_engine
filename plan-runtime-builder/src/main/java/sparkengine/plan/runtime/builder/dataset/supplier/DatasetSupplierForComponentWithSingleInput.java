package sparkengine.plan.runtime.builder.dataset.supplier;

import org.apache.spark.sql.types.StructType;
import sparkengine.plan.model.component.ComponentWithSingleInput;
import sparkengine.plan.model.component.impl.EncodeComponent;
import sparkengine.plan.model.component.impl.MapComponent;
import sparkengine.plan.model.component.impl.SchemaValidationComponent;
import sparkengine.plan.runtime.builder.dataset.utils.EncoderUtils;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.transformation.DataTransformation;
import sparkengine.spark.transformation.Transformations;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;

@Value
@Builder
public class DatasetSupplierForComponentWithSingleInput<T> implements DatasetSupplier<T> {

    @Nonnull
    SparkSession sparkSession;
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

    private Dataset<T> getMapDataset(MapComponent txComponent) throws DatasetFactoryException {
        DataTransformation<Object, T> dxTransformation = null;

        try {
            dxTransformation = (DataTransformation<Object, T>) Class.forName(txComponent.getTransformWith()).getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            throw new DatasetFactoryException("unable to instantiate map with class: [" + txComponent.getTransformWith() + "]");
        }

        if (txComponent.getEncodedAs() != null) {
            var encoder = (Encoder<T>) EncoderUtils.buildEncoder(txComponent.getEncodedAs());
            dxTransformation = dxTransformation.andThenEncode(encoder);
        }

        return dxTransformation.apply(inputDataset);
    }

}
