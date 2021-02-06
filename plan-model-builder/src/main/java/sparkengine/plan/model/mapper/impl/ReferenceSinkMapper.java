package sparkengine.plan.model.mapper.impl;

import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.mapper.PlanMapper;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ForeachSink;
import sparkengine.plan.model.sink.mapper.SinkMapper;

public class ReferenceSinkMapper implements SinkMapper {

    PlanMapper planMapper;

    @Override
    public Sink mapForeachSink(ForeachSink sink) throws Exception {
        var plan = sink.getPlan();
        var newPlan = planMapper.map(plan);
        return sink.withPlan(newPlan);
    }
}
