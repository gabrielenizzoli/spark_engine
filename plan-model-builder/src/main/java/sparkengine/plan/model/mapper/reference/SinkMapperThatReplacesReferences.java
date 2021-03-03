package sparkengine.plan.model.mapper.reference;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.common.Reference;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.builder.ResourceLocationBuilder;
import sparkengine.plan.model.plan.mapper.SinkMapperForComponents;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ReferenceSink;
import sparkengine.plan.model.sink.mapper.SinksMapper;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;

public class SinkMapperThatReplacesReferences extends SinkMapperForComponents {

    public static final String PLAN = "plan";

    @Nonnull
    private final ResourceLocationBuilder resourceLocationBuilder;
    @Nonnull
    private final InputStreamResourceLocator resourceLocator;

    public static SinkMapperThatReplacesReferences of(@Nonnull ComponentMapper componentMapper,
                                                      @Nonnull ResourceLocationBuilder resourceLocationBuilder,
                                                      @Nonnull InputStreamResourceLocator resourceLocator) {
        return new SinkMapperThatReplacesReferences(componentMapper, resourceLocationBuilder, resourceLocator);
    }

    private SinkMapperThatReplacesReferences(@Nonnull ComponentMapper componentMapper,
                                             @Nonnull ResourceLocationBuilder resourceLocationBuilder,
                                             @Nonnull InputStreamResourceLocator resourceLocator) {
        super(componentMapper);
        this.resourceLocationBuilder = Objects.requireNonNull(resourceLocationBuilder);
        this.resourceLocator = Objects.requireNonNull(resourceLocator);
    }

    @Override
    public Sink mapReferenceSink(Location location, ReferenceSink sink) throws Exception {

        if (sink.getMode() == Reference.ReferenceMode.ABSOLUTE) {
            String uri = sink.getRef();
            var inputStream = resourceLocator.getInputStreamFactory(uri);
            var newSink = ModelFactory.readSinkFromYaml(inputStream);
            var sinkMapper = SinkMapperThatReplacesReferences.of(componentMapper, resourceLocationBuilder.withRoot(uri), resourceLocator);
            return SinksMapper.mapSink(Location.empty(), sinkMapper, newSink);
        } else if (sink.getMode() == Reference.ReferenceMode.RELATIVE) {
            var effectiveLocation = Optional
                    .ofNullable(sink.getRef())
                    .map(String::strip)
                    .filter(ref -> !ref.isBlank())
                    .map(Location::of)
                    .orElse(location);
            String uri = resourceLocationBuilder.build(effectiveLocation);
            var inputStream = resourceLocator.getInputStreamFactory(uri);
            var newComponent = ModelFactory.readSinkFromYaml(inputStream);
            return SinksMapper.mapSink(effectiveLocation, this, newComponent);
        }

        return sink;
    }

}
