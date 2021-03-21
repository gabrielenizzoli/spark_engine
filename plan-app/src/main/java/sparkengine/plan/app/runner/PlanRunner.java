package sparkengine.plan.app.runner;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.ModelFormatException;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.plan.mapper.PlanMapperException;
import sparkengine.plan.runtime.builder.runner.ModelPipelineRunnersFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.plan.runtime.runner.PipelineRunnersFactory;
import sparkengine.plan.runtime.runner.PipelineRunnersFactoryException;

import javax.annotation.Nonnull;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.function.Consumer;

@Value
@Builder
public class PlanRunner {

    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    PlanInfo planInfo;
    @Nonnull
    @lombok.Builder.Default
    RuntimeArgs runtimeArgs = RuntimeArgs.builder().build();
    @Nonnull
    @lombok.Builder.Default
    Logger log = Logger.getLogger(PlanRunner.class);

    public static class Builder {

        public Builder planLocation(String planLocation) {
            return this.planInfo(PlanInfo.planLocation(planLocation));
        }

    }

    public void run() throws
            IOException, // can't read plan
            ModelFormatException, // plan is bad
            PlanMapperException, // can't resolve plan
            PipelineRunnersFactoryException, // error creating a pipeline
            DatasetConsumerException // error during a pipeline run
    {
        Plan plan = getPlan();
        PipelineRunnersFactory pipelineRunnersFactory = getPipelineRunnersFactory(plan);
        if (runtimeArgs.isSkipRun()) {
            log.warn("skip run");
        } else {
            executePipelines(pipelineRunnersFactory);
            waitOnSpark();
        }
    }

    @Nonnull
    private Plan getPlan() throws IOException, ModelFormatException, PlanMapperException {
        var sourcePlan = ModelFactory.readPlanFromYaml(planInfo.getPlanInputStreamFactory());
        log.trace(String.format("source plan [%s]", sourcePlan));
        var resolvedPlan = PlanResolver.of(planInfo.getPlanLocation(), runtimeArgs, sparkSession, log).map(sourcePlan);
        writeResolvedPlan(resolvedPlan);
        return resolvedPlan;
    }

    private PipelineRunnersFactory getPipelineRunnersFactory(@Nonnull Plan plan) throws IOException, PlanMapperException, ModelFormatException {
        return ModelPipelineRunnersFactory.ofPlan(sparkSession, plan);
    }

    private void writeResolvedPlan(Plan resolvedPlan) throws IOException, ModelFormatException {
        if (runtimeArgs.getWriteResolvedPlanToFile() != null) {
            var resolvedFilePlan = runtimeArgs.getWriteResolvedPlanToFile();
            log.warn(String.format("writing resolved plan to [%s]", resolvedFilePlan));
            try (var out = new FileOutputStream(resolvedFilePlan)) {
                ModelFactory.writePlanAsYaml(resolvedPlan, out);
            }
        }

        if (runtimeArgs.isWriteResolvedPlan()) {
            ModelFactory.writePlanAsYaml(resolvedPlan, System.out);
        }
    }

    private void executePipelines(PipelineRunnersFactory pipelineRunnersFactory)
            throws PipelineRunnersFactoryException, DatasetConsumerException {

        var pipelinesDeclared = pipelineRunnersFactory.getPipelineNames();
        var pipelinesToExecute = new LinkedList<>(pipelinesDeclared);
        if (runtimeArgs.getPipelines() != null) {
            pipelinesToExecute = new LinkedList<>(runtimeArgs.getPipelines());
            pipelinesToExecute.retainAll(pipelinesDeclared);
        }
        log.info(String.format("found pipelines [%s] (user override: %b; parallel execution: %b)",
                pipelinesToExecute,
                runtimeArgs.getPipelines() != null,
                runtimeArgs.isParallelPipelineExecution()));

        var pipelines = runtimeArgs.isParallelPipelineExecution() ? pipelinesToExecute.parallelStream() : pipelinesToExecute.stream();
        var runPipeline = (Consumer<String>) pipelineName -> runPipeline(pipelineRunnersFactory, pipelineName);

        pipelines.forEach(runPipeline);
    }

    @SneakyThrows
    private void runPipeline(PipelineRunnersFactory pipelineRunnersFactory,
                             String pipelineName) {
        log.info(String.format("running pipeline [%s]", pipelineName));
        try {
            var pipeline = pipelineRunnersFactory.buildPipelineRunner(pipelineName);
            pipeline.run();
        } catch (PipelineRunnersFactoryException e) {
            String msg = String.format("can't instantiate pipeline [%s]", pipelineName);
            if (runtimeArgs.isSkipFaultyPipelines())
                log.warn(msg, e);
            else
                throw new PipelineRunnersFactoryException(msg, e);
        } catch (DatasetConsumerException e) {
            String msg = String.format("can't execute pipeline [%s]", pipelineName);
            if (runtimeArgs.isSkipFaultyPipelines())
                log.warn(msg, e);
            else
                throw new DatasetConsumerException(msg, e);
        }
    }

    private void waitOnSpark() throws IOException {
        if (sparkSession.sessionState().streamingQueryManager().active().length > 0) {
            try {
                log.info("waiting for any stream to finish");
                sparkSession.sessionState().streamingQueryManager().awaitAnyTermination();
            } catch (StreamingQueryException e) {
                throw new IOException(e);
            }
        }
    }

}
