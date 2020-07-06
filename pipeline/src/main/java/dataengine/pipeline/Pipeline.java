package dataengine.pipeline;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactory;

public class Pipeline {

    String source;
    String sink;
    DataSourceCatalog dataSourceCatalog;
    DataSinkFactory<?> dataSinkFactory;

    public void run( ) throws DataSourceCatalogException {
        DataSource<?> dataSource = dataSourceCatalog.lookup(source);
        DataSink dataSink = dataSinkFactory.get();
        dataSource.writeTo(dataSink);
    }
}
