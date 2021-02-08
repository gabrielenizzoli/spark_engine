package sparkengine.plan.model.plan.mapper;

import sparkengine.plan.model.plan.Plan;

import javax.annotation.Nonnull;

public interface PlanMapper {

    @Nonnull
    Plan map(@Nonnull Plan plan) throws PlanMapperException;

}
