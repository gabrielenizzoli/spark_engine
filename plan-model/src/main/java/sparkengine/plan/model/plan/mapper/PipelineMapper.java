package sparkengine.plan.model.plan.mapper;

import sparkengine.plan.model.plan.Pipeline;

import java.util.Map;

public interface PipelineMapper {

    Map<String, Pipeline> mapPipelines(Map<String, Pipeline> pipelines) throws Exception;

}
