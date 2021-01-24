package sparkengine.plan.model.resolver;

import sparkengine.plan.model.Plan;

public interface PlanResolver {

    Plan resolve(Plan plan) throws PlanResolverException;

}
