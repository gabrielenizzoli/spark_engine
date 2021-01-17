package dataengine.pipeline.runtime.builder.dataset;

import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.ComponentWithMultipleInputs;
import dataengine.pipeline.model.component.ComponentWithNoInput;
import dataengine.pipeline.model.component.ComponentWithSingleInput;
import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.component.catalog.ComponentCatalogException;
import dataengine.pipeline.runtime.builder.dataset.supplier.DatasetSupplier;
import dataengine.pipeline.runtime.builder.dataset.supplier.DatasetSupplierForComponentWithMultipleInput;
import dataengine.pipeline.runtime.builder.dataset.supplier.DatasetSupplierForComponentWithNoInput;
import dataengine.pipeline.runtime.builder.dataset.supplier.DatasetSupplierForComponentWithSingleInput;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactory;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
@Builder
public class ComponentDatasetFactory implements DatasetFactory {

    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    ComponentCatalog componentCatalog;
    @lombok.Builder.Default
    @With
    Map<String, Dataset> datasetCache = new HashMap<>();

    public static ComponentDatasetFactory of(SparkSession sparkSession, ComponentCatalog catalog) {
        return ComponentDatasetFactory.builder().sparkSession(sparkSession).componentCatalog(catalog).build();
    }


    @Nonnull
    @Override
    public <T> Dataset<T> buildDataset(String name) throws DatasetFactoryException {
        return buildDataset(name, List.of());
    }

    private <T> Dataset<T> buildDataset(@Nonnull String name, @Nonnull List<String> childrenPath) throws DatasetFactoryException {

        if (name == null || name.isBlank())
            throw new DatasetFactoryException("dataset name is null or blank: [" + name + "]");
        name = name.strip();
        if (childrenPath == null)
            throw new DatasetFactoryException("children path is empty");

        if (datasetCache.containsKey(name))
            return (Dataset<T>) datasetCache.get(name);

        if (childrenPath.contains(name))
            throw new DatasetFactoryException.DatasetCircularReference("component " + name + " already in path " + childrenPath + " (execution path has a circular reference)");

        var component = getComponent(name);
        Dataset<T> ds = getDataset(name, component, childrenPath);

        datasetCache.put(name, (Dataset<Object>) ds);
        return ds;
    }

    @Nonnull
    private Component getComponent(String name) throws DatasetFactoryException {
        try {
            return componentCatalog.lookup(name).orElseThrow(() -> new DatasetFactoryException.DatasetNotFound(name));
        } catch (ComponentCatalogException e) {
            throw new DatasetFactoryException("issues locating component with name " + name, e);
        }
    }

    @Nonnull
    private <T> Dataset<T> getDataset(String name, Component component, List<String> childrenPath) throws DatasetFactoryException {

        DatasetSupplier<T> datasetSupplier = null;
        if (component instanceof ComponentWithNoInput) {
            datasetSupplier = DatasetSupplierForComponentWithNoInput.<T>builder()
                    .sparkSession(sparkSession)
                    .componentWithNoInput((ComponentWithNoInput) component)
                    .build();
        } else if (component instanceof ComponentWithSingleInput) {
            var componentWithSingleInput = (ComponentWithSingleInput) component;
            var parentDs = getParentDataset(componentWithSingleInput.getUsing(), appendToPath(name, childrenPath));
            datasetSupplier = DatasetSupplierForComponentWithSingleInput.<T>builder()
                    .sparkSession(sparkSession)
                    .componentWithSingleInput(componentWithSingleInput)
                    .inputDataset(parentDs)
                    .build();
        } else if (component instanceof ComponentWithMultipleInputs) {
            var multiInputComponent = (ComponentWithMultipleInputs) component;
            var parentDs = getParentDatasets(multiInputComponent.getUsing(), appendToPath(name, childrenPath));
            datasetSupplier = DatasetSupplierForComponentWithMultipleInput.<T>builder()
                    .sparkSession(sparkSession)
                    .componentWithMultipleInputs(multiInputComponent)
                    .inputDatasets(parentDs)
                    .build();
        }

        if (datasetSupplier == null)
            throw new DatasetFactoryException.DatasetInstantiationIssue("component type [" + component.getClass().getName() + "] does not have any supplier associated");
        Dataset<T> ds = datasetSupplier.provides();
        if (ds == null)
            throw new DatasetFactoryException.DatasetInstantiationIssue("component type [" + component.getClass().getName() + "] supplier unable to create dataset");

        return ds;
    }

    private <T> Dataset<T> getParentDataset(@Nonnull String parentName, @Nonnull List<String> childrenPath) throws DatasetFactoryException {
        return buildDataset(parentName, childrenPath);
    }

    @Nonnull
    private List<Dataset<Object>> getParentDatasets(@Nullable List<String> parentNames, List<String> childrenPath) throws DatasetFactoryException {
        if (parentNames == null) {
            parentNames = List.of();
        }
        var parentDs = new ArrayList<Dataset<Object>>(parentNames.size());
        for (String parentName : parentNames) {
            parentDs.add(getParentDataset(parentName, childrenPath));
        }
        return parentDs;
    }

    @Nonnull
    private static List<String> appendToPath(String name, List<String> childrenPath) {
        return Stream.concat(childrenPath.stream(), Stream.of(name)).collect(Collectors.toUnmodifiableList());
    }

}
