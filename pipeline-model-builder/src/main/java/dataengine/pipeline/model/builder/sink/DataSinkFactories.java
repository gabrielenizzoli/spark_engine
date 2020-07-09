package dataengine.pipeline.model.builder.sink;

import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.sink.factory.DataSinkFactoryException;
import dataengine.pipeline.model.description.sink.BatchSink;
import dataengine.pipeline.model.description.sink.ShowSink;
import dataengine.pipeline.model.description.sink.Sink;

import javax.annotation.Nonnull;

public class DataSinkFactories {

    public static DataSinkFactory<?> factoryFor(@Nonnull Sink sink)
            throws DataSinkFactoryException {
        if (sink instanceof ShowSink) {
            return new ShowSinkFactory<>((ShowSink)sink);
        } else if (sink instanceof BatchSink) {
            return new BatchSinkFactory<>((BatchSink) sink);
        }
        throw new DataSinkFactoryException("sink " + sink + " not managed");

    }

}
