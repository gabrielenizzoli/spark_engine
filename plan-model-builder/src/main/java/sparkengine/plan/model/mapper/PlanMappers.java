package sparkengine.plan.model.mapper;

import lombok.Value;
import sparkengine.plan.model.Plan;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

@Value
public class PlanMappers implements PlanMapper {

    @Nonnull
    List<PlanMapper> planMappers;

    public static PlanMapper ofMappers(PlanMapper... planMappers) {
        return new PlanMappers(Arrays.asList(planMappers));
    }

    @Override
    public @Nonnull Plan map(@Nonnull Plan plan) throws PlanMapperException {

        for (var planResolver : planMappers)
            plan = planResolver.map(plan);

        return plan;
    }
}
