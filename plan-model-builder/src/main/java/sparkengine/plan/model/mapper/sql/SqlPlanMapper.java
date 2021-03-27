package sparkengine.plan.model.mapper.sql;

import sparkengine.plan.model.plan.mapper.DefaultPlanMapper;
import sparkengine.plan.model.plan.mapper.PlanMapper;
import sparkengine.plan.model.sink.mapper.SinkMapperForComponents;

public class SqlPlanMapper {

    public static PlanMapper of(ResolverMode resolverMode, SqlReferenceFinder sqlReferenceFinder) {
        var sqlComponentMapper = SqlComponentMapper.of(resolverMode, sqlReferenceFinder);
        return DefaultPlanMapper.builder()
                .componentMapper(sqlComponentMapper)
                .sinkMapper(new SinkMapperForComponents(sqlComponentMapper))
                .build();
    }

}
