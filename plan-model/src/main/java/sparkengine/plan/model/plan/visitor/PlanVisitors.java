package sparkengine.plan.model.plan.visitor;

import lombok.Value;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.plan.mapper.PlanMapper;
import sparkengine.plan.model.plan.mapper.PlanMapperException;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

@Value
public class PlanVisitors implements PlanMapper {

    @Nonnull
    List<PlanMapper> planMappers;

    public static PlanMapper ofMappers(PlanMapper... planMappers) {
        return new PlanVisitors(Arrays.asList(planMappers));
    }

    @Override
    public @Nonnull Plan map(@Nonnull Plan plan) throws PlanMapperException {

        for (var planResolver : planMappers)
            plan = planResolver.map(plan);

        return plan;
    }
}
