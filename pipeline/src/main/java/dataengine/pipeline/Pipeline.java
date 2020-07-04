package dataengine.pipeline;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;

public class Pipeline {

    String source;
    String sink;
    DataSourceFactory dataSourceFactory;
    DataSinkFactory dataSinkFactory;

    public void run( ){
        DataSource<?> dataSource = dataSourceFactory.apply(source);
        DataSink dataSink = dataSinkFactory.apply(sink);
        dataSource.writeTo(dataSink);
    }
}
