package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.composer.DataSourceComposer;
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
        } else if (source instanceof EmptyDatasetSource) {
            return new EmptyDatasetSourceFactory((EmptyDatasetSource) source);
        } else if (source instanceof InlineDataframeSource) {
            return new InlineDataframeSourceFactory((InlineDataframeSource) source);
        }
        throw new DataSourceFactoryException("source " + source + " not managed");
    }

    public static DataSourceFactory<?> factoryForSingleInputComponent(
            @Nonnull TransformationComponentWithSingleInput component,
            @Nonnull DataSourceComposer dataSourceComposer)
            throws DataSourceFactoryException {
        if (component instanceof Encode) {
            return new EncodeFactory((Encode) component, dataSourceComposer);
        }
        throw new DataSourceFactoryException("component " + component + " not managed");
    }

    public static DataSourceFactory<?> factoryForMultiInputComponent(
            @Nonnull TransformationComponentWithMultipleInputs component,
            @Nonnull DataSourceComposer catalog)
            throws DataSourceFactoryException {
        if (component instanceof Union) {
            return new UnionFactory((Union) component, catalog);
        } else if (component instanceof Sql) {
            return new SqlFactory((Sql) component, catalog);
        }
        throw new DataSourceFactoryException("component " + component + " not managed");
    }

    public static DataSourceFactory<?> factoryForComponent(
            @Nonnull Component component,
            @Nonnull DataSourceComposer dataSourceComposer)
            throws DataSourceFactoryException {
        if (component instanceof SourceComponent) {
            return factoryForSourceComponent((SourceComponent) component);
        } else if (component instanceof TransformationComponentWithSingleInput) {
            return factoryForSingleInputComponent((TransformationComponentWithSingleInput) component, dataSourceComposer);
        } else if (component instanceof TransformationComponentWithMultipleInputs) {
            return factoryForMultiInputComponent((TransformationComponentWithMultipleInputs) component, dataSourceComposer);
        }
        throw new DataSourceFactoryException("component " + component + " not managed");
    }

}
