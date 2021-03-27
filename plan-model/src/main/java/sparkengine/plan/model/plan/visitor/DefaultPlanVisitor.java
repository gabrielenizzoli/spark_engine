package sparkengine.plan.model.plan.visitor;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.visitor.ComponentVisitor;
import sparkengine.plan.model.component.visitor.ComponentsVisitor;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.visitor.SinkVisitor;
import sparkengine.plan.model.sink.visitor.SinksVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder
public class DefaultPlanVisitor implements PlanVisitor {

    public static final String COMPONENTS = "components";
    public static final String SINKS = "sinks";

    @Nullable
    ComponentVisitor componentVisitor;
    @Nullable
    SinkVisitor sinkVisitor;
    @Nonnull
    @Builder.Default
    Location location = Location.empty();

    @Override
    public @Nonnull
    void visit(@Nonnull Plan plan) throws PlanVisitorException {
        visitAllComponents(plan.getComponents());
        visitAllSinks(plan.getSinks());
    }

    private void visitAllComponents(@Nonnull Map<String, Component> components)
            throws PlanVisitorException {

        if (componentVisitor == null)
            return;

        try {
            ComponentsVisitor.visitComponents(location.push(COMPONENTS), componentVisitor, components);
        } catch (Exception | ComponentsVisitor.InternalVisitorError e) {
            throw new PlanVisitorException("exception resolving plan with resolver " + this.getClass().getName(), e);
        }
    }

    private void visitAllSinks(@Nonnull Map<String, Sink> sinks)
            throws PlanVisitorException {

        if (sinkVisitor == null)
            return;

        try {
            SinksVisitor.visitSinks(location.push(SINKS), sinkVisitor, sinks);
        } catch (Exception | SinksVisitor.InternalVisitorError e) {
            throw new PlanVisitorException("exception resolving plan with resolver " + this.getClass().getName(), e);
        }
    }

}
