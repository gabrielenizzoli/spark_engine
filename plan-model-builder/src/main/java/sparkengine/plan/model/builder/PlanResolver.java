package sparkengine.plan.model.builder;

import sparkengine.plan.model.Plan;

import java.io.IOException;

public interface PlanResolver {

    Plan resolve(Plan plan) throws IOException, PlanResolverException;

}
