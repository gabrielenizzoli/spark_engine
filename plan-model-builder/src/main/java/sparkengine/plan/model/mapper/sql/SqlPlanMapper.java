package sparkengine.plan.model.mapper.sql;

import sparkengine.plan.model.mapper.DefaultPlanMapper;
import sparkengine.plan.model.mapper.PlanMapper;
import sparkengine.plan.model.mapper.SinkMapperForComponents;

public class SqlPlanMapper {

    public static PlanMapper of(ResolverMode resolverMode, SqlReferenceFinder sqlReferenceFinder) {
        var sqlComponentMapper = SqlComponentMapper.of(resolverMode, sqlReferenceFinder);
        return DefaultPlanMapper.builder()
                .componentMapper(sqlComponentMapper)
                .sinkMapper(new SinkMapperForComponents(sqlComponentMapper))
                .build();
    }

}
