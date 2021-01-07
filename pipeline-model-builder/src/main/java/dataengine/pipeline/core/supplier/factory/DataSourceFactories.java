package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalog;
import dataengine.pipeline.model.source.Component;
import dataengine.pipeline.model.source.SourceComponent;
import dataengine.pipeline.model.source.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.source.TransformationComponentWithSingleInput;
import dataengine.pipeline.model.source.component.*;

import javax.annotation.Nonnull;

public class DataSourceFactories {

    public static DatasetSupplierFactory<?> factoryForSourceComponent(@Nonnull SourceComponent source)
            throws DatasetSupplierFactoryException {
        if (source instanceof SqlSource) {
            return new SqlSourceFactory((SqlSource) source);
        } else if (source instanceof BatchSource) {
            return new BatchSourceFactory((BatchSource) source);
        } else if (source instanceof StreamSource) {
            return new StreamSourceFactory((StreamSource) source);
        } else if (source instanceof EmptySource) {
            return new EmptyDatasetSourceFactory((EmptySource) source);
        } else if (source instanceof InlineSource) {
            return new InlineDataframeSourceFactory((InlineSource) source);
        }
        throw new DatasetSupplierFactoryException("source " + source + " not managed");
    }

    public static DatasetSupplierFactory<?> factoryForSingleInputComponent(
            @Nonnull TransformationComponentWithSingleInput component,
            @Nonnull DatasetSupplierCatalog DatasetSupplierCatalog)
            throws DatasetSupplierFactoryException {
        if (component instanceof Encode) {
            return new EncodeFactory((Encode) component, DatasetSupplierCatalog);
        }
        throw new DatasetSupplierFactoryException("component " + component + " not managed");
    }

    public static DatasetSupplierFactory<?> factoryForMultiInputComponent(
            @Nonnull TransformationComponentWithMultipleInputs component,
            @Nonnull DatasetSupplierCatalog catalog)
            throws DatasetSupplierFactoryException {
        if (component instanceof Union) {
            return new UnionFactory((Union) component, catalog);
        } else if (component instanceof Sql) {
            return new SqlFactory((Sql) component, catalog);
        } else if (component instanceof PlaceholderSchemaComponent) {
            return new SqlFactory((Sql) component, catalog);
        }
        throw new DatasetSupplierFactoryException("component " + component + " not managed");
    }

    public static DatasetSupplierFactory<?> factoryForComponent(
            @Nonnull Component component,
            @Nonnull DatasetSupplierCatalog DatasetSupplierCatalog)
            throws DatasetSupplierFactoryException {
        if (component instanceof SourceComponent) {
            return factoryForSourceComponent((SourceComponent) component);
        } else if (component instanceof TransformationComponentWithSingleInput) {
            return factoryForSingleInputComponent((TransformationComponentWithSingleInput) component, DatasetSupplierCatalog);
        } else if (component instanceof TransformationComponentWithMultipleInputs) {
            return factoryForMultiInputComponent((TransformationComponentWithMultipleInputs) component, DatasetSupplierCatalog);
        }
        throw new DatasetSupplierFactoryException("component " + component + " not managed");
    }

}
