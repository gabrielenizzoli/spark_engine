package sparkengine.plan.app;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.builder.DefaultReferencePlanResolver;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.ModelFormatException;
import sparkengine.plan.model.builder.PlanResolverException;
import sparkengine.plan.model.builder.input.AbsoluteResourceLocator;
import sparkengine.plan.model.builder.input.InputStreamFactory;
import sparkengine.plan.model.builder.input.RelativeResourceLocator;
import sparkengine.plan.model.builder.input.URIBuilder;
import sparkengine.plan.runtime.PipelineName;
import sparkengine.plan.runtime.PipelineRunnersFactory;
import sparkengine.plan.runtime.PipelineRunnersFactoryException;
import sparkengine.plan.runtime.builder.ModelPipelineRunnersFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.function.Consumer;

@Value
@Builder
public class PlanRunner {

    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    RuntimeArgs runtimeArgs;
    @Nonnull
    @lombok.Builder.Default
    Logger log = Logger.getLogger(PlanRunner.class);

    public void run() throws
            IOException, // cant read plan
            ModelFormatException, // plan is bad
            PlanResolverException, // cant resolve plan
            PipelineRunnersFactoryException, // error creating pipeline
            DatasetConsumerException // error during run
    {
        PipelineRunnersFactory pipelineRunnersFactory = getPipelineRunnersFactory();
        executePipelines(pipelineRunnersFactory);
        waitOnSpark();
    }

    private PipelineRunnersFactory getPipelineRunnersFactory() throws IOException, PlanResolverException, ModelFormatException {
        var resourceLocator = new AbsoluteResourceLocator();
        var planInputStream = resourceLocator.getInputStreamFactory(runtimeArgs.getPlanLocation());
        var sourcePlan = getPlan(planInputStream);
        Plan resolvedPlan = resolvePlan(sourcePlan);
        return ModelPipelineRunnersFactory.ofPlan(sparkSession, resolvedPlan);
    }

    private Plan getPlan(InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        var sourcePlan = ModelFactory.readPlanFromYaml(inputStreamFactory);
        log.trace("source plan: " + sourcePlan);

        return sourcePlan;
    }

    private Plan resolvePlan(Plan sourcePlan) throws PlanResolverException {
        var relativeResourceLocator = RelativeResourceLocator.builder()
                .baseLocation(URIBuilder.ofString(runtimeArgs.getPlanLocation()).removePartFromPath().getUri())
                .extension("yaml")
                .build();
        var resolver = DefaultReferencePlanResolver.builder()
                .relativeResourceLocator(relativeResourceLocator)
                .build();
        var resolvedPlan = resolver.resolve(sourcePlan);
        log.trace("resolved plan: " + resolvedPlan);

        return resolvedPlan;
    }

    private void executePipelines(PipelineRunnersFactory pipelineRunnersFactory)
            throws PipelineRunnersFactoryException, DatasetConsumerException {
        log.info("found pipelines " + pipelineRunnersFactory.getPipelineNames());
        Consumer<PipelineName> runPipeline = pipelineName -> runPipeline(pipelineRunnersFactory, pipelineName);
        if (runtimeArgs.isParallelPipelineExecution()) {
            log.info("running pipelines in parallel");
            pipelineRunnersFactory.getPipelineNames().parallelStream().forEach(runPipeline::accept);
        } else {
            pipelineRunnersFactory.getPipelineNames().forEach(runPipeline::accept);
        }
    }

    @SneakyThrows
    private void runPipeline(PipelineRunnersFactory pipelineRunnersFactory,
                             PipelineName pipelineName) {
        log.info("running pipeline " + pipelineName);
        try {
            var pipeline = pipelineRunnersFactory.buildPipelineRunner(pipelineName);
            pipeline.run();
        } catch (PipelineRunnersFactoryException e) {
            String msg = "can't instantiate pipeline " + pipelineName;
            if (runtimeArgs.isSkipFaultyPipelines())
                log.warn(msg, e);
            else
                throw new PipelineRunnersFactoryException(msg, e);
        } catch (DatasetConsumerException e) {
            String msg = "can't execute pipeline " + pipelineName;
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
