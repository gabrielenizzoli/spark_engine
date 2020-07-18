package dataengine.pipeline;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.sink.factory.DataSinkFactoryException;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.core.source.composer.DataSourceComposerException;

public class Pipeline {

    String source;
    DataSourceComposer dataSourceComposer;
    DataSinkFactory<?> dataSinkFactory;

    public void run( ) throws DataSourceComposerException, DataSinkFactoryException {
        DataSource<?> dataSource = dataSourceComposer.lookup(source);
        DataSink dataSink = dataSinkFactory.build();
        dataSource.writeTo(dataSink);
    }

}
