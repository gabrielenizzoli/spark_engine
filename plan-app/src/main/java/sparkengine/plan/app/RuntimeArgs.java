package sparkengine.plan.app;

import com.beust.jcommander.Parameter;
import lombok.Getter;

public class RuntimeArgs {

    @Getter
    @Parameter(names = {"-p", "-planLocation"}, required = true, order = 2, description = "Location of the execution plan (in yaml format)")
    private String planLocation;

    @Getter
    @Parameter(names = {"-parallelPipelineExecution"}, order = 3, description = "Executes the pipelines of the plan in parallel (instead of sequentially)")
    private boolean parallelPipelineExecution = false;

    @Getter
    @Parameter(names = {"-skipFaultyPipelines"}, order = 3, description = "Skip a faulty pipeline (instead of exiting the application)")
    private boolean skipFaultyPipelines = false;

}
