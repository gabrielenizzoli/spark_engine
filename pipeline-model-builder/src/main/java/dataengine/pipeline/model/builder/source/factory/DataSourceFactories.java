package dataengine.pipeline.model.builder.source.factory;

import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.SourceComponent;
import dataengine.pipeline.model.description.source.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.description.source.TransformationComponentWithSingleInput;
import dataengine.pipeline.model.description.source.component.*;

import javax.annotation.Nonnull;

public class DataSourceFactories {

    public static DataSourceFactory<?> factoryForSourceComponent(@Nonnull SourceComponent source)
            throws DataSourceFactoryException {
        if (source instanceof SqlSource) {
            return new SqlSourceFactory((SqlSource) source);
        } else if (source instanceof BatchSource) {
            return new BatchSourceFactory((BatchSource) source);
        } else if (source instanceof StreamSource) {
            return new StreamSourceFactory((StreamSource) source);
        }
        throw new DataSourceFactoryException("source " + source + " not managed");
    }

    public static DataSourceFactory<?> factoryForSingleInputComponent(
            @Nonnull TransformationComponentWithSingleInput component,
            @Nonnull DataSourceCatalog catalog)
            throws DataSourceFactoryException {
        if (component instanceof Encode) {
            return new EncodeFactory((Encode) component, catalog);
        } else if (component instanceof Sql) {
            return new SqlFactory((Sql) component, catalog);
        }
        throw new DataSourceFactoryException("component " + component + " not managed");
    }

    public static DataSourceFactory<?> factoryForMultiInputComponent(
            @Nonnull TransformationComponentWithMultipleInputs component,
            @Nonnull DataSourceCatalog catalog)
            throws DataSourceFactoryException {
        if (component instanceof Union) {
            return new UnionFactory((Union) component, catalog);
        } else if (component instanceof SqlMerge) {
            return new SqlMergeFactory((SqlMerge) component, catalog);
        }
        throw new DataSourceFactoryException("component " + component + " not managed");
    }

    public static DataSourceFactory<?> factoryForComponent(
            @Nonnull Component component,
            @Nonnull DataSourceCatalog catalog)
            throws DataSourceFactoryException {
        if (component instanceof SourceComponent) {
            return factoryForSourceComponent((SourceComponent) component);
        } else if (component instanceof TransformationComponentWithSingleInput) {
            return factoryForSingleInputComponent((TransformationComponentWithSingleInput) component, catalog);
        } else if (component instanceof TransformationComponentWithMultipleInputs) {
            return factoryForMultiInputComponent((TransformationComponentWithMultipleInputs) component, catalog);
        }
        throw new DataSourceFactoryException("component " + component + " not managed");
    }

}
