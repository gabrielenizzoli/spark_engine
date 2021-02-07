package sparkengine.plan.model.mapper.reference;

import sparkengine.plan.model.builder.input.InputStreamResourceLocator;
import sparkengine.plan.model.plan.mapper.DefaultPlanMapper;
import sparkengine.plan.model.plan.mapper.PlanMapper;
import sparkengine.plan.model.builder.ResourceLocationBuilder;

import javax.annotation.Nonnull;

public class ReferencePlanMapper {

    public static PlanMapper of(@Nonnull ResourceLocationBuilder resourceLocationBuilder,
                                @Nonnull InputStreamResourceLocator resourceLocator) {
        var componentMapper = ReferenceComponentMapper.of(resourceLocationBuilder, resourceLocator);
        var sinkMapper = new ReferenceSinkMapper(componentMapper, resourceLocationBuilder, resourceLocator);
        return DefaultPlanMapper.builder()
                .componentMapper(componentMapper)
                .sinkMapper(sinkMapper)
                .build();
    }

}
