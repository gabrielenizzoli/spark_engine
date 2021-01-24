package sparkengine.plan.model.resolver;

import lombok.Value;
import sparkengine.plan.model.Plan;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

@Value
public class PlanResolvers implements PlanResolver {

    @Nonnull
    List<PlanResolver> planResolvers;

    public static PlanResolver ofResolvers(PlanResolver... planResolvers) {
        return new PlanResolvers(Arrays.asList(planResolvers));
    }

    @Override
    public Plan resolve(Plan plan) throws PlanResolverException {

        for (var planResolver : planResolvers)
            plan = planResolver.resolve(plan);

        return plan;
    }
}
