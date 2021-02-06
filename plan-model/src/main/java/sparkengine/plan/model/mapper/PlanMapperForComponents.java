package sparkengine.plan.model.mapper;

import lombok.Value;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.component.mapper.ComponentsMapper;
import sparkengine.plan.model.LocationUtils;

import javax.annotation.Nonnull;
import java.util.Map;

@Value(staticConstructor = "of")
public class PlanMapperForComponents implements PlanMapper {

    @Nonnull
    ComponentMapper componentMapper;

    @Override
    public @Nonnull
    Plan map(@Nonnull Plan plan) throws PlanMapperException {
        var components = mapAllComponents(plan.getComponents());
        return plan.withComponents(components);
    }

    private Map<String, Component> mapAllComponents(@Nonnull Map<String, Component> components)
            throws PlanMapperException {
        try {
            return ComponentsMapper.mapComponents(LocationUtils.empty(), componentMapper, components);
        } catch (Exception | ComponentsMapper.InternalMapperError e) {
            throw new PlanMapperException("exception resolving pan with resolver " + this.getClass().getName(), e);
        }
    }

}
