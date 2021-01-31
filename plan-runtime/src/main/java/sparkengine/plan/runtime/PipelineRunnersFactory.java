package sparkengine.plan.runtime;

import java.util.Set;

public interface PipelineRunnersFactory {

    Set<String> getPipelineNames();

    PipelineRunner buildPipelineRunner(String pipelineName) throws PipelineRunnersFactoryException;

}
