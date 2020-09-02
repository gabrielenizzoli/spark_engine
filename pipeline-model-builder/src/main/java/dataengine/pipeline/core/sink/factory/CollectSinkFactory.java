package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.impl.DataSinkCollect;

public class CollectSinkFactory<T> implements DataSinkFactory<T> {

    @Override
    public DataSink<T> build() throws DataSinkFactoryException {
        return new DataSinkCollect<>();
    }

}
