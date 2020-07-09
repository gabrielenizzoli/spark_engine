package dataengine.pipeline;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.sink.factory.DataSinkFactoryException;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;

public class Pipeline {

    String source;
    DataSourceCatalog dataSourceCatalog;
    DataSinkFactory<?> dataSinkFactory;

    public void run( ) throws DataSourceCatalogException, DataSinkFactoryException {
        DataSource<?> dataSource = dataSourceCatalog.lookup(source);
        DataSink dataSink = dataSinkFactory.build();
        dataSource.writeTo(dataSink);
    }

}
