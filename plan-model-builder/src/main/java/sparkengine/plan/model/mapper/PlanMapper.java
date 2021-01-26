package sparkengine.plan.model.mapper;

import sparkengine.plan.model.Plan;

import javax.annotation.Nonnull;

public interface PlanMapper {

    @Nonnull
    Plan map(@Nonnull Plan plan) throws PlanMapperException;

}
