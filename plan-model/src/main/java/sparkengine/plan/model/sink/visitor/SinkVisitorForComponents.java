package sparkengine.plan.model.sink.visitor;

import lombok.AllArgsConstructor;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.component.visitor.ComponentVisitor;
import sparkengine.plan.model.plan.mapper.DefaultPlanMapper;
import sparkengine.plan.model.plan.visitor.DefaultPlanVisitor;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ForeachSink;
import sparkengine.plan.model.sink.mapper.SinkMapper;

import javax.annotation.Nonnull;

@AllArgsConstructor
public class SinkVisitorForComponents implements SinkVisitor {

    public static final String PLAN = "plan";

    @Nonnull
    protected final ComponentVisitor componentVisitor;

    @Override
    public final void visitForeachSink(Location location, ForeachSink sink) throws Exception {

        var plan = sink.getPlan();

        var planMapper = DefaultPlanVisitor.builder()
                .componentVisitor(componentVisitor)
                .sinkVisitor(this)
                .location(location.push(PLAN))
                .build();

        planMapper.visit(plan);
    }

}
