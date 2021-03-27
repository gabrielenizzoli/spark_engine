package sparkengine.plan.model.plan.visitor;

import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.plan.mapper.PlanMapperException;

import javax.annotation.Nonnull;

public interface PlanVisitor {

    void visit(@Nonnull Plan plan) throws PlanVisitorException;

}
