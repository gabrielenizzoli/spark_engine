package sparkengine.plan.runtime.builder.datasetconsumer;

import sparkengine.plan.model.component.ComponentWithNoRuntime;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.SinkWithNoRuntime;
import sparkengine.plan.model.sink.catalog.SinkCatalog;
import sparkengine.plan.model.sink.catalog.SinkCatalogException;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumer;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactoryException;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.sink.impl.*;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;

import javax.annotation.Nonnull;

@Value
@Builder
public class SinkDatasetConsumerFactory implements DatasetConsumerFactory {

    @Nonnull
    SinkCatalog sinkCatalog;

    public static SinkDatasetConsumerFactory of(SinkCatalog catalog) {
        return SinkDatasetConsumerFactory.builder().sinkCatalog(catalog).build();
    }

    @Override
    public <T> DatasetConsumer<T> buildConsumer(String consumerName) throws DatasetConsumerFactoryException {

        if (consumerName == null || consumerName.isBlank())
            throw new DatasetConsumerFactoryException("consumer name is null or blank: [" + consumerName + "]");
        consumerName = consumerName.strip();

        var sink = getSink(consumerName);
        return getConsumer(sink);
    }

    @Nonnull
    private Sink getSink(String name) throws DatasetConsumerFactoryException {
        try {
            return sinkCatalog.lookup(name).orElseThrow(() -> new DatasetConsumerFactoryException.ConsumerNotFound(name));
        } catch (SinkCatalogException e) {
            throw new DatasetConsumerFactoryException("issues locating sink with name " + name, e);
        }
    }

    private <T> DatasetConsumer<T> getConsumer(Sink sink) throws DatasetConsumerFactoryException {

        if (sink instanceof SinkWithNoRuntime) {
            throw new DatasetConsumerFactoryException.ConsumerInstantiationException(String.format("sink [%s] has no runtime equivalent and must be resolved", sink));
        }

        if (sink instanceof ShowSink) {
            var show = (ShowSink) sink;
            return (DatasetConsumer<T>) ShowConsumer.builder()
                    .count(show.getNumRows())
                    .truncate(show.getTruncate())
                    .build();
        }

        if (sink instanceof ViewSink) {
            var view = (ViewSink) sink;
            return ViewConsumer.of(view.getName());
        }

        if (sink instanceof CounterSink) {
            var counter = (CounterSink)sink;
            return GlobalCounterConsumer.<T>builder().key(counter.getKey()).build();
        }

        if (sink instanceof BatchSink) {
            return BatchConsumer.<T>of(WriterFormatter.getBatchFormatter((BatchSink) sink));
        }

        if (sink instanceof StreamSink) {
            var stream = (StreamSink) sink;
            return StreamConsumer.<T>of(WriterFormatter.getStreamFormatter(stream));
        }

        if (sink instanceof ForeachSink) {
            var foreach = (ForeachSink)sink;
            return ForeachConsumer.<T>builder()
                    .batchComponentName(foreach.getBatchComponentName())
                    .formatter(WriterFormatter.getForeachFormatter(foreach))
                    .plan(foreach.getPlan()).build();
        }

        throw new DatasetConsumerFactoryException.ConsumerInstantiationException("sink type [" + sink.getClass().getName() + "] does not have any factory associated");

    }

}
