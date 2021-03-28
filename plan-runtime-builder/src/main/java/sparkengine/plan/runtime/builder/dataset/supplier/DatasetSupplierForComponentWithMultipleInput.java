package sparkengine.plan.runtime.builder.dataset.supplier;

import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;
import sparkengine.plan.model.component.ComponentWithSingleInput;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.component.impl.*;
import sparkengine.plan.runtime.builder.RuntimeContext;
import sparkengine.plan.runtime.builder.dataset.ComponentDatasetFactory;
import sparkengine.plan.runtime.builder.dataset.utils.EncoderUtils;
import sparkengine.plan.runtime.builder.dataset.utils.TransformationUtils;
import sparkengine.plan.runtime.builder.dataset.utils.UdfUtils;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.transformation.DataTransformationWithEncoder;
import sparkengine.spark.transformation.Transformations;
import sparkengine.spark.transformation.context.DataTransformationWithContext;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Value
@Builder
public class DatasetSupplierForComponentWithMultipleInput<T> implements DatasetSupplier<T> {

    @Nonnull
    RuntimeContext runtimeContext;
    @Nonnull
    ComponentWithMultipleInputs componentWithMultipleInputs;
    @Nonnull
    List<Dataset> inputDatasets;

    @Override
    public Dataset<T> getDataset() throws DatasetFactoryException {
        Dataset<T> dataset = null;

        if (componentWithMultipleInputs instanceof UnionComponent) {
            dataset = getUnionDataset();
        } else if (componentWithMultipleInputs instanceof FragmentComponent) {
            dataset =  getFragmentDataset((FragmentComponent) componentWithMultipleInputs);
        } else if (componentWithMultipleInputs instanceof WrapperComponent) {
            dataset =  getWrappedDataset((WrapperComponent) componentWithMultipleInputs);
        } else if (componentWithMultipleInputs instanceof SqlComponent) {
            dataset =  getSqlDataset((SqlComponent) componentWithMultipleInputs);
        } else if (componentWithMultipleInputs instanceof TransformComponent) {
            dataset =  getTransformDataset((TransformComponent) componentWithMultipleInputs);
        }

        return dataset;
    }

    private Dataset<T> getUnionDataset() throws DatasetFactoryException {
        return inputDatasets.stream()
                .map(ds -> (Dataset<T>) ds)
                .reduce(Dataset::union)
                .orElseThrow(() -> new DatasetFactoryException("union can't be performed on an empty list of datasets"));
    }

    @Nonnull
    private Dataset<T> getFragmentDataset(FragmentComponent fragmentComponent) throws DatasetFactoryException {
        var componentCatalog = ComponentCatalog.ofMap(fragmentComponent.getComponents());
        var datasetCache = Optional.ofNullable(fragmentComponent.getUsing())
                .map(names -> IntStream.range(0, names.size()).boxed().collect(Collectors.toMap(names::get, inputDatasets::get)))
                .orElse(Map.of());
        var factory = ComponentDatasetFactory.of(runtimeContext, componentCatalog).withDatasetCache(datasetCache);
        return factory.buildDataset(fragmentComponent.getProviding());
    }

    @Nonnull
    private Dataset<T> getWrappedDataset(WrapperComponent wrapper) throws DatasetFactoryException {
        var providedDatasetNames = Optional.ofNullable(wrapper.getUsing()).orElse(List.of());
        var innerComponent = wrapper.getComponent();

        var requestedComponents = List.<String>of();
        if (innerComponent instanceof ComponentWithMultipleInputs) {
            requestedComponents = Optional.ofNullable(((ComponentWithMultipleInputs) innerComponent).getUsing()).orElse(List.of());
        } else if (innerComponent instanceof ComponentWithSingleInput) {
            requestedComponents = Optional.ofNullable(((ComponentWithSingleInput) innerComponent).getUsing()).map(List::of).orElse(List.of());
        }

        if (providedDatasetNames.size() != requestedComponents.size()) {
            throw new DatasetFactoryException("wrapper list of provided source names (" + providedDatasetNames + ") does not match size of required source names in inner component (" + requestedComponents + ")");
        }

        var datasetCache = Optional.ofNullable(requestedComponents)
                .filter(names -> !names.isEmpty())
                .map(names -> IntStream.range(0, names.size()).boxed().collect(Collectors.toMap(names::get, inputDatasets::get)))
                .orElse(Map.of());

        var componentCatalog = ComponentCatalog.ofMap(Map.of("wrappedComponent", innerComponent));
        var factory = ComponentDatasetFactory.of(runtimeContext, componentCatalog).withDatasetCache(datasetCache);
        return factory.<T>buildDataset("wrappedComponent");
    }

    private Dataset<T> getSqlDataset(SqlComponent sqlComponent) throws DatasetFactoryException {
        var sqlFunctions = UdfUtils.buildSqlFunctionCollection(sqlComponent.getUdfs(), runtimeContext);
        var rowEncoder = Transformations.encodeAsRow();
        var parentDf = inputDatasets.stream().map(ds -> (Dataset<Object>) ds).map(rowEncoder::apply).collect(Collectors.toList());

        var tx = Transformations.sql(runtimeContext.getSparkSession(), sqlComponent.getUsing(), sqlComponent.getSql(), sqlFunctions);
        if (sqlComponent.getEncodedAs() != null) {
            var encode = EncoderUtils.buildEncoder(sqlComponent.getEncodedAs());
            tx = tx.andThenEncode(encode);
        }

        return (Dataset<T>) tx.apply(parentDf);
    }

    private Dataset<T> getTransformDataset(TransformComponent transformComponent) throws DatasetFactoryException {
        var dataTransformation = TransformationUtils.<T>getDataTransformationN(transformComponent.getTransformWith());
        TransformationUtils.injectTransformationParameters(dataTransformation, transformComponent.getParams());

        if (dataTransformation instanceof DataTransformationWithContext) {
            var txWithContext = (DataTransformationWithContext)dataTransformation;
            txWithContext.setTransformationContext(runtimeContext.buildBroadcastTransformationContext(transformComponent.getAccumulators()));
        }

        if (transformComponent.getEncodedAs() != null) {
            var encoder = (Encoder<T>) EncoderUtils.buildEncoder(transformComponent.getEncodedAs());
            if (dataTransformation instanceof DataTransformationWithEncoder) {
                var dataTransformationWithEncoder = (DataTransformationWithEncoder<T>)dataTransformation;
                dataTransformationWithEncoder.setEncoder(encoder);
            } else {
                dataTransformation = dataTransformation.andThenEncode(encoder);
            }
        }

        return dataTransformation.apply(inputDatasets.stream().map(m -> (Dataset<Object>) m).collect(Collectors.toList()));
    }

}
