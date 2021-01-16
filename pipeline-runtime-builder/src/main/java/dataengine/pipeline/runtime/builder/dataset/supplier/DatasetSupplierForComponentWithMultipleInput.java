package dataengine.pipeline.runtime.builder.dataset.supplier;

import dataengine.pipeline.model.component.ComponentWithMultipleInputs;
import dataengine.pipeline.model.component.impl.SqlComponent;
import dataengine.pipeline.model.component.impl.TransformComponent;
import dataengine.pipeline.model.component.impl.UnionComponent;
import dataengine.pipeline.runtime.builder.dataset.utils.EncoderUtils;
import dataengine.pipeline.runtime.builder.dataset.utils.UdfUtils;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.spark.transformation.DataTransformationN;
import dataengine.spark.transformation.Transformations;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

@Value
@Builder
public class DatasetSupplierForComponentWithMultipleInput<T> implements DatasetSupplier<T> {

    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    ComponentWithMultipleInputs componentWithMultipleInputs;
    @Nonnull
    List<Dataset<Object>> inputDatasets;

    @Override
    public Dataset<T> provides() throws DatasetFactoryException {
        if (componentWithMultipleInputs instanceof UnionComponent) {
            return inputDatasets.stream()
                    .map(ds -> (Dataset<T>) ds)
                    .reduce(Dataset::union)
                    .orElseThrow(() -> new DatasetFactoryException("union can't be performed on an empty list of datasets"));
        }
        if (componentWithMultipleInputs instanceof SqlComponent) {
            return getSqlDataset((SqlComponent) componentWithMultipleInputs);
        }
        if (componentWithMultipleInputs instanceof TransformComponent) {
            return getTransformDataset((TransformComponent) componentWithMultipleInputs);
        }

        return null;
    }

    private Dataset<T> getSqlDataset(SqlComponent sqlComponent) throws DatasetFactoryException {
        var sqlFunctions = UdfUtils.buildSqlFunctionCollection(sqlComponent.getUdfs());
        var rowEncoder = Transformations.encodeAsRow();
        var parentDf = inputDatasets.stream().map(ds -> (Dataset<Object>) ds).map(rowEncoder::apply).collect(Collectors.toList());

        var tx = Transformations.sql(sparkSession, sqlComponent.getUsing(), sqlComponent.getSql(), sqlFunctions);
        if (sqlComponent.getEncodedAs() != null) {
            var encode = EncoderUtils.buildEncoder(sqlComponent.getEncodedAs());
            tx = tx.andThenEncode(encode);
        }

        return (Dataset<T>) tx.apply(parentDf);
    }

    private Dataset<T> getTransformDataset(TransformComponent txComponent) throws DatasetFactoryException {
        DataTransformationN<Object, T> dxTransformation = null;

        try {
            dxTransformation = (DataTransformationN<Object, T>) Class.forName(txComponent.getTransformWith()).getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            throw new DatasetFactoryException("unable to instantiate transformation with class: [" + txComponent.getTransformWith() + "]");
        }

        if (txComponent.getEncodedAs() != null) {
            var encoder = (Encoder<T>) EncoderUtils.buildEncoder(txComponent.getEncodedAs());
            dxTransformation = dxTransformation.andThenEncode(encoder);
        }

        return dxTransformation.apply(inputDatasets.stream().map(m -> (Dataset<Object>) m).collect(Collectors.toList()));
    }

}
