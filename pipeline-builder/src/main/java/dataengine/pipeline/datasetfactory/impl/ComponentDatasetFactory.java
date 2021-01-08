package dataengine.pipeline.datasetfactory.impl;

import dataengine.pipeline.datasetfactory.DatasetFactory;
import dataengine.pipeline.datasetfactory.DatasetFactoryException;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.SourceComponent;
import dataengine.pipeline.model.component.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.component.TransformationComponentWithSingleInput;
import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.component.catalog.ComponentCatalogException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

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
    ComponentCatalog componentCatalog;
    @lombok.Builder.Default
    Map<String, Dataset<?>> datasetCache = new HashMap<>();

    public static ComponentDatasetFactory of(ComponentCatalog catalog) {
        return ComponentDatasetFactory.builder().componentCatalog(catalog).build();
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

        datasetCache.put(name, ds);
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
        Dataset<T> ds = null;
        if (component instanceof SourceComponent) {
            ds = Factories.getSourceComponentDataset((SourceComponent) component);
        } else if (component instanceof TransformationComponentWithSingleInput) {
            var singleInputComponent = (TransformationComponentWithSingleInput) component;
            Dataset<Object> parentDs = getParentDataset(singleInputComponent.getUsing(), appendToPath(name, childrenPath));
            ds = Factories.getSingleInputComponent(singleInputComponent, parentDs);
        } else if (component instanceof TransformationComponentWithMultipleInputs) {
            var multiInputComponent = (TransformationComponentWithMultipleInputs) component;
            List<Dataset<?>> parentDs = getParentDatasets(multiInputComponent.getUsing(), appendToPath(name, childrenPath));
            ds = Factories.getMultiInputComponentDataset(multiInputComponent, parentDs);
        }

        if (ds == null)
            throw new DatasetFactoryException.DatasetInstantiationIssue("component type [" + component.getClass().getName() + "] does not have any factory associated");

        return ds;
    }

    private <T> Dataset<T> getParentDataset(@Nonnull String parentName, @Nonnull List<String> childrenPath) throws DatasetFactoryException {
        return buildDataset(parentName, childrenPath);
    }

    @Nonnull
    private List<Dataset<?>> getParentDatasets(@Nullable List<String> parentNames, List<String> childrenPath) throws DatasetFactoryException {
        if (parentNames == null) {
            parentNames = List.of();
        }
        var parentDs = new ArrayList<Dataset<?>>(parentNames.size());
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
