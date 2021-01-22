package sparkengine.plan.app;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.builder.DefaultPLanResolver;
import sparkengine.plan.model.builder.PlanResolverException;
import sparkengine.plan.model.builder.input.*;
import sparkengine.plan.model.builder.ModelFactories;
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

    public void run() throws IOException, PlanResolverException {
        PipelineRunnersFactory pipelineRunnersFactory = getPipelineRunnersFactory();
        executePipelines(pipelineRunnersFactory);
        waitOnSpark();
    }

    private PipelineRunnersFactory getPipelineRunnersFactory() throws IOException, PlanResolverException {
        var resourceLocator = new AppResourceLocator();
        var planInputStream =  resourceLocator.getInputStreamSupplier(runtimeArgs.getPlanLocation());
        var sourcePlan = getPlan(planInputStream);
        Plan resolvedPlan = resolvePlan(sourcePlan);
        return ModelPipelineRunnersFactory.ofPlan(sparkSession, resolvedPlan);
    }

    private Plan getPlan(InputStreamSupplier inputStreamSupplier) throws IOException {
        var sourcePlan = ModelFactories.readPlanFromYaml(inputStreamSupplier);
        log.trace("source plan: " + sourcePlan);

        return sourcePlan;
    }

    private Plan resolvePlan(Plan sourcePlan) throws PlanResolverException, IOException {
        var relativeResourceLocator = RelativeResourceLocator.builder()
                .baseLocation(URIBuilder.ofString(runtimeArgs.getPlanLocation()).removePartFromPath().getUri())
                .extension("yaml")
                .build();
        var absoluteResourceLocator = AbsoluteResourceLocator.builder()
                .extension("yaml")
                .build();
        var resolver = DefaultPLanResolver.builder()
                .relativeResourceLocator(relativeResourceLocator)
                .absoluteResourceLocator(absoluteResourceLocator)
                .build();
        var resolvedPlan = resolver.resolve(sourcePlan);
        log.trace("resolved plan: " + resolvedPlan);

        return resolvedPlan;
    }

    private void executePipelines(PipelineRunnersFactory pipelineRunnersFactory) throws IOException {
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
                throw new IOException(msg, e);
        } catch (DatasetConsumerException e) {
            String msg = "can't execute pipeline " + pipelineName;
            if (runtimeArgs.isSkipFaultyPipelines())
                log.warn(msg, e);
            else
                throw new IOException(msg, e);
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
