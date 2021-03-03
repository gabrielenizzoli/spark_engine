package sparkengine.plan.model.mapper.reference;

import sparkengine.plan.model.builder.ResourceLocationBuilder;
import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
import sparkengine.plan.model.plan.mapper.DefaultPlanMapper;
import sparkengine.plan.model.plan.mapper.PlanMapper;

import javax.annotation.Nonnull;

public class PlanMapperThatReplacesReferences {

    public static PlanMapper of(@Nonnull ResourceLocationBuilder resourceLocationBuilder,
                                @Nonnull InputStreamResourceLocator resourceLocator) {
        var componentMapper = ComponentMapperThatReplacesReferences.of(resourceLocationBuilder, resourceLocator);
        var sinkMapper = SinkMapperThatReplacesReferences.of(componentMapper, resourceLocationBuilder, resourceLocator);
        return DefaultPlanMapper.builder()
                .componentMapper(componentMapper)
                .sinkMapper(sinkMapper)
                .build();
    }

}
