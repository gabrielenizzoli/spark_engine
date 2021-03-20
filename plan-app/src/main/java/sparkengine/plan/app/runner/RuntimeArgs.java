package sparkengine.plan.app.runner;

import com.beust.jcommander.Parameter;
import lombok.*;
import sparkengine.plan.model.mapper.sql.ResolverMode;

import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RuntimeArgs {

    @Getter
    @Parameter(names = {"-s", "--sqlResolution"}, order = 2, description = "For sql components, provide validation and/or dependency discovery")
    @lombok.Builder.Default
    private ResolverMode sqlResolutionMode = ResolverMode.VALIDATE;

    @Getter
    @Parameter(names = {"--pipelines"}, order = 3, description = "Provide an list of pipelines to execute (if pipeline is not in plan, it will be ignored)")
    @lombok.Builder.Default
    private List<String> pipelines = null;

    @Getter
    @Parameter(names = {"--skipRun"}, order = 3, description = "Do everything, but do not run the pipelines")
    @lombok.Builder.Default
    private boolean skipRun = false;

    @Getter
    @Parameter(names = {"--skipResolution"}, description = "Skip any resolution of the plan (plan will be executed as-is!)")
    @lombok.Builder.Default
    private boolean skipResolution = false;

    @Getter
    @Parameter(names = {"--skipFaultyPipelines"}, description = "Skip a faulty pipeline (instead of exiting the application)")
    @lombok.Builder.Default
    private boolean skipFaultyPipelines = false;

    @Getter
    @Parameter(names = {"--parallelPipelineExecution"}, description = "Executes the pipelines of the plan in parallel (instead of sequentially)")
    @lombok.Builder.Default
    private boolean parallelPipelineExecution = false;

    @Getter
    @Parameter(names = {"--writeResolvedPlan"}, description = "Write the resolved plan (to standard output)")
    @lombok.Builder.Default
    private boolean writeResolvedPlan = false;

    @Getter
    @Parameter(names = {"--writeResolvedPlanToFile"}, description = "Write the resolved plan to the specified plan")
    @lombok.Builder.Default
    private String writeResolvedPlanToFile = null;

}
