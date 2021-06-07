package sparkengine.plan.model.mapper.parameters;

import sparkengine.plan.model.plan.mapper.DefaultPlanMapper;
import sparkengine.plan.model.plan.mapper.PlanMapper;
import sparkengine.plan.model.sink.mapper.SinkMapperForComponents;

import java.util.Map;

public class ParameterReplacerPlanMapper {

    public static PlanMapper of(Map<String, String> params, String prefix, String postfix) {
        var parameterReplacerComponentMapper = ParameterReplacerComponentMapper.of(params, prefix, postfix);
        return DefaultPlanMapper.builder()
                .componentMapper(parameterReplacerComponentMapper)
                .sinkMapper(new SinkMapperForComponents(parameterReplacerComponentMapper))
                .build();
    }

}
