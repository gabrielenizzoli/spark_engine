package sparkengine.plan.model.mapper.parameters;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.BatchSink;
import sparkengine.plan.model.sink.impl.StreamSink;
import sparkengine.plan.model.sink.mapper.SinkMapperForComponents;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ParameterReplacerSinkMapper extends SinkMapperForComponents {

    @Nonnull
    private final Map<String, String> parameters;
    @Nonnull
    private final String prefix;
    @Nonnull
    private final String postfix;

    public ParameterReplacerSinkMapper(@Nonnull ComponentMapper componentMapper,
                                       @Nonnull Map<String, String> parameters,
                                       @Nonnull String prefix,
                                       @Nonnull String postfix) {
        super(componentMapper);
        this.parameters = Objects.requireNonNull(parameters);
        this.prefix = Objects.requireNonNull(prefix);
        this.postfix = Objects.requireNonNull(postfix);
    }

    @Override
    public Sink mapBatchSink(Location location, BatchSink sink) throws Exception {
        if (sink == null || sink.getOptions() == null || sink.getOptions().isEmpty()) {
            return sink;
        }

        var options = sink.getOptions();
        HashMap<String, String> newOptions = ReplacementTemplateParser.mapOptions(options, prefix, postfix, parameters);

        return sink.withOptions(newOptions);
    }

    @Override
    public Sink mapStreamSink(Location location, StreamSink sink) throws Exception {
        if (sink == null || sink.getOptions() == null || sink.getOptions().isEmpty()) {
            return sink;
        }

        var options = sink.getOptions();
        HashMap<String, String> newOptions = ReplacementTemplateParser.mapOptions(options, prefix, postfix, parameters);

        return sink.withOptions(newOptions);    }
}
