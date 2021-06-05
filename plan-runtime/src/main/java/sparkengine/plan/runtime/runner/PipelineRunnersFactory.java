package sparkengine.plan.runtime.runner;

import java.util.List;
import java.util.Set;

public interface PipelineRunnersFactory {

    List<String> getPipelineNames();

    PipelineRunner buildPipelineRunner(String pipelineName) throws PipelineRunnersFactoryException;

}
