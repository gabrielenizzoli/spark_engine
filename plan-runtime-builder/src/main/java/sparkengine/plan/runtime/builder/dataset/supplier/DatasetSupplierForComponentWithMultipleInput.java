package sparkengine.plan.runtime.builder.dataset.supplier;

import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;
import sparkengine.plan.model.component.ComponentWithSingleInput;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.component.impl.*;
import sparkengine.plan.runtime.builder.RuntimeContext;
import sparkengine.plan.runtime.builder.dataset.ComponentDatasetFactory;
import sparkengine.plan.runtime.builder.dataset.utils.EncoderUtils;
import sparkengine.plan.runtime.builder.dataset.utils.UdfContextFactory;
import sparkengine.plan.runtime.builder.dataset.utils.UdfUtils;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.transformation.DataTransformationN;
import sparkengine.spark.transformation.DataTransformationWithParameters;
import sparkengine.spark.transformation.Transformations;

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

    private Dataset<T> getTransformDataset(TransformComponent txComponent) throws DatasetFactoryException {
        DataTransformationN<Object, T> dxTransformation = null;

        try {
            dxTransformation = (DataTransformationN<Object, T>) Class.forName(txComponent.getTransformWith()).getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            throw new DatasetFactoryException(String.format("unable to instantiate transformation with class [%s]", txComponent.getTransformWith()), e);
        }

        if (dxTransformation instanceof DataTransformationWithParameters) {

            var dxWithParameters = (DataTransformationWithParameters)dxTransformation;
            var parameterType = dxWithParameters.getParametersType();
            if (txComponent.getParams() == null || txComponent.getParams().isEmpty()) {
                throw new DatasetFactoryException(String.format("transformation class [%s] expects a parameter class [%s], but no parameters provided",
                        dxTransformation.getClass().getName(),
                        parameterType.getName()));
            }

            try {
                var params = new ObjectMapper().convertValue(txComponent.getParams(), parameterType);
                dxWithParameters.setParameters(params);
            } catch (Exception e) {
                throw new DatasetFactoryException(String.format("parameters for transformation class [%s] do not fit parameter class [%s]",
                        dxTransformation.getClass().getName(),
                        parameterType.getName()),
                        e);
            }
        } else if (txComponent.getParams() != null && !txComponent.getParams().isEmpty()) {
            throw new DatasetFactoryException(String.format("transformation with class [%s] does not accepts parameters, but parameters provided [%s]",
                    dxTransformation.getClass().getName(),
                    txComponent.getParams()));
        }

        if (txComponent.getEncodedAs() != null) {
            var encoder = (Encoder<T>) EncoderUtils.buildEncoder(txComponent.getEncodedAs());
            dxTransformation = dxTransformation.andThenEncode(encoder);
        }

        return dxTransformation.apply(inputDatasets.stream().map(m -> (Dataset<Object>) m).collect(Collectors.toList()));
    }

}
