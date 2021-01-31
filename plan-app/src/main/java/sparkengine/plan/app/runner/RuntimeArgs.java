package sparkengine.plan.app.runner;

import com.beust.jcommander.Parameter;
import lombok.*;
import sparkengine.plan.model.mapper.impl.SqlComponentMapper;

import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RuntimeArgs {

    @Getter
    @Parameter(names = {"-p", "--planLocation"}, required = true, order = 1, description = "Location of the execution plan (in yaml format)")
    private String planLocation;

    @Getter
    @Parameter(names = {"-s", "--sqlResolution"}, order = 2, description = "For sql components, provide validation and/or dependency discovery")
    @lombok.Builder.Default
    private SqlComponentMapper.ResolverMode sqlResolutionMode = SqlComponentMapper.ResolverMode.VALIDATE;

    @Getter
    @Parameter(names = {"--pipelines"}, order = 3, description = "Provide an list of pipelines to execute (if pipeline is not in plan, it will be ignored)")
    @lombok.Builder.Default
    private List<String> pipelines = null;

    @Getter
    @Parameter(names = {"--skipRun"}, order = 3, description = "Do everything, but do not run the pipelines")
    @lombok.Builder.Default
    private boolean skipRun = false;

    @Getter
    @Parameter(names = {"--skipFaultyPipelines"}, order = 4, description = "Skip a faulty pipeline (instead of exiting the application)")
    @lombok.Builder.Default
    private boolean skipFaultyPipelines = false;

    @Getter
    @Parameter(names = {"--parallelPipelineExecution"}, order = 4, description = "Executes the pipelines of the plan in parallel (instead of sequentially)")
    @lombok.Builder.Default
    private boolean parallelPipelineExecution = false;

}
