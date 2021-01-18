package sparkengine.plan.runtime.builder.dataset.supplier;

import sparkengine.plan.model.component.ComponentWithSingleInput;
import sparkengine.plan.model.component.impl.EncodeComponent;
import sparkengine.plan.runtime.builder.dataset.utils.EncoderUtils;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
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
    Dataset<Object> inputDataset;

    @Override
    public Dataset<T> provides() throws DatasetFactoryException {
        if (componentWithSingleInput instanceof EncodeComponent) {
            var encodeComponent = (EncodeComponent) componentWithSingleInput;
            Encoder<?> encoder = EncoderUtils.buildEncoder(encodeComponent.getEncodedAs());
            return (Dataset<T>) Transformations.encodeAs(encoder).apply(inputDataset);
        }

        return null;
    }

}
