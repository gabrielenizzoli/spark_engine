package sparkengine.plan.model.mapper.reference;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.LocationUtils;
import sparkengine.plan.model.Reference;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.mapper.ResourceLocationBuilder;
import sparkengine.plan.model.mapper.SinkMapperForComponents;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ForeachSink;
import sparkengine.plan.model.sink.impl.ReferenceSink;
import sparkengine.plan.model.sink.mapper.SinkMapper;
import sparkengine.plan.model.sink.mapper.SinksMapper;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;

public class ReferenceSinkMapper extends SinkMapperForComponents {

    public static final String PLAN = "plan";

    @Nonnull
    private final ResourceLocationBuilder resourceLocationBuilder;
    @Nonnull
    private final InputStreamResourceLocator resourceLocator;

    public ReferenceSinkMapper(@Nonnull ComponentMapper componentMapper,
                               @Nonnull ResourceLocationBuilder resourceLocationBuilder,
                               @Nonnull InputStreamResourceLocator resourceLocator) {
        super(componentMapper);
        this.resourceLocationBuilder = Objects.requireNonNull(resourceLocationBuilder);
        this.resourceLocator = Objects.requireNonNull(resourceLocator);
    }

    @Override
    public Sink mapReferenceSink(Stack<String> location, ReferenceSink sink) throws Exception {

        if (sink.getMode() == Reference.ReferenceMode.ABSOLUTE) {
            String uri = sink.getRef();
            var inputStream = resourceLocator.getInputStreamFactory(uri);
            var newSink = ModelFactory.readSinkFromYaml(inputStream);
            var sinkMapper = new ReferenceSinkMapper(componentMapper, resourceLocationBuilder.withRoot(uri), resourceLocator);
            return SinksMapper.mapSink(LocationUtils.empty(), sinkMapper, newSink);
        } else if (sink.getMode() == Reference.ReferenceMode.RELATIVE) {
            var effectiveLocation = Optional
                    .ofNullable(sink.getRef())
                    .map(String::strip)
                    .filter(ref -> !ref.isBlank())
                    .map(LocationUtils::of)
                    .orElse(location);
            String uri = resourceLocationBuilder.build(effectiveLocation);
            var inputStream = resourceLocator.getInputStreamFactory(uri);
            var newComponent = ModelFactory.readSinkFromYaml(inputStream);
            return SinksMapper.mapSink(effectiveLocation, this, newComponent);
        }

        return sink;
    }

}
