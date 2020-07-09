package dataengine.pipeline.model.builder.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.core.source.impl.EmptyDataframeSource;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class EmptyDataframeSourceFactory implements DataSourceFactory {

    @Nonnull
    dataengine.pipeline.model.description.source.component.EmptyDataframeSource source;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        return EmptyDataframeSource.builder()
                .schema(source.getSchema())
                .build();
    }

}
