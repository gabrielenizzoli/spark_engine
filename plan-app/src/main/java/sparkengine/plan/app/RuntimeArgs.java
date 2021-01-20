package sparkengine.plan.app;

import com.beust.jcommander.Parameter;
import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RuntimeArgs {

    @Getter
    @Parameter(names = {"-p", "-planLocation"}, required = true, order = 1, description = "Location of the execution plan (in yaml format)")
    private String planLocation;

    @Getter
    @Parameter(names = {"-parallelPipelineExecution"}, order = 2, description = "Executes the pipelines of the plan in parallel (instead of sequentially)")
    @lombok.Builder.Default
    private boolean parallelPipelineExecution = false;

    @Getter
    @Parameter(names = {"-skipFaultyPipelines"}, order = 2, description = "Skip a faulty pipeline (instead of exiting the application)")
    @lombok.Builder.Default
    private boolean skipFaultyPipelines = false;

}
