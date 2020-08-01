package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceError;
import dataengine.pipeline.model.description.source.component.PlaceholderSchemaComponent;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class PlaceholderSourceFactory implements DataSourceFactory {

    @Nonnull
    PlaceholderSchemaComponent source;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        return () -> {
            throw new DataSourceError("this is a placeholder for a datasource with a schema: " + source.getSchema());
        };
    }

}
