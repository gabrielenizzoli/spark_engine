package sparkengine.plan.app.runner;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.ModelFormatException;
import sparkengine.plan.model.builder.input.AppResourceLocator;
import sparkengine.plan.model.builder.input.InputStreamFactory;
import sparkengine.plan.model.mapper.*;
import sparkengine.plan.model.mapper.impl.ReferenceComponentMapper;
import sparkengine.plan.model.mapper.impl.SqlComponentMapper;
import sparkengine.plan.runtime.PipelineRunnersFactory;
import sparkengine.plan.runtime.PipelineRunnersFactoryException;
import sparkengine.plan.runtime.builder.ModelPipelineRunnersFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.spark.sql.logicalplan.PlanExplorerException;
import sparkengine.spark.sql.logicalplan.tablelist.TableListExplorer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.LinkedList;
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
            IOException, // can't read plan
            ModelFormatException, // plan is bad
            PlanMapperException, // can't resolve plan
            PipelineRunnersFactoryException, // error creating a pipeline
            DatasetConsumerException // error during a pipeline run
    {
        PipelineRunnersFactory pipelineRunnersFactory = getPipelineRunnersFactory();
        if (runtimeArgs.isSkipRun()) {
            log.info("skip run option enabled");
        } else {
            executePipelines(pipelineRunnersFactory);
            waitOnSpark();
        }
    }

    private PipelineRunnersFactory getPipelineRunnersFactory() throws IOException, PlanMapperException, ModelFormatException {
        var resourceLocator = new AppResourceLocator();
        var planInputStream = resourceLocator.getInputStreamFactory(runtimeArgs.getPlanLocation());
        var sourcePlan = getPlan(planInputStream);
        var resolvedPlan = resolvePlan(sourcePlan);
        return ModelPipelineRunnersFactory.ofPlan(sparkSession, resolvedPlan);
    }

    private Plan getPlan(InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        var sourcePlan = ModelFactory.readPlanFromYaml(inputStreamFactory);
        log.trace("source plan: " + sourcePlan);

        return sourcePlan;
    }

    private Plan resolvePlan(Plan sourcePlan) throws PlanMapperException {
        var resolvedPlan = PlanMappers
                .ofMappers(
                        getReferencePlanResolver(),
                        getSqlResolver())
                .map(sourcePlan);

        if (log.isTraceEnabled())
            log.trace(String.format("resolved plan: %s", resolvedPlan));

        return resolvedPlan;
    }

    private PlanMapper getReferencePlanResolver() {
        var resourceLocationBuilder = new ComponentResourceLocationBuilder(runtimeArgs.getPlanLocation(), "_", "yaml");
        var resourceLocator = new AppResourceLocator();
        var referenceMapper = ReferenceComponentMapper.of(resourceLocationBuilder, resourceLocator);

        return ComponentsPlanMapper.of(referenceMapper);
    }

    private PlanMapper getSqlResolver() {
        var sqlComponentResolver = SqlComponentMapper.of(runtimeArgs.getSqlResolutionMode(), sql -> {
            try {
                return TableListExplorer.findTableListInSql(sparkSession, sql);
            } catch (PlanExplorerException e) {
                throw new PlanMapperException(String.format("error resolving sql tables in sql [%s]", sql));
            }
        });
        return ComponentsPlanMapper.of(sqlComponentResolver);
    }

    private void executePipelines(PipelineRunnersFactory pipelineRunnersFactory)
            throws PipelineRunnersFactoryException, DatasetConsumerException {

        var pipelinesDeclared = pipelineRunnersFactory.getPipelineNames();
        var pipelinesToExecute = new LinkedList<>(pipelinesDeclared);
        if (runtimeArgs.getPipelines() != null) {
            pipelinesToExecute = new LinkedList<>(runtimeArgs.getPipelines());
            pipelinesToExecute.retainAll(pipelinesDeclared);
        }
        log.info(String.format("found pipelines %s (user override: %b; parallel execution: %b)",
                pipelinesToExecute,
                runtimeArgs.getPipelines() != null,
                runtimeArgs.isParallelPipelineExecution()));

        var pipelines = runtimeArgs.isParallelPipelineExecution() ? pipelinesToExecute.parallelStream() : pipelinesToExecute.stream();
        var runPipeline = (Consumer<String>)pipelineName -> runPipeline(pipelineRunnersFactory, pipelineName);

        pipelines.forEach(runPipeline);
    }

    @SneakyThrows
    private void runPipeline(PipelineRunnersFactory pipelineRunnersFactory,
                             String pipelineName) {
        log.info(String.format("running pipeline %s", pipelineName));
        try {
            var pipeline = pipelineRunnersFactory.buildPipelineRunner(pipelineName);
            pipeline.run();
        } catch (PipelineRunnersFactoryException e) {
            String msg = String.format("can't instantiate pipeline %s", pipelineName);
            if (runtimeArgs.isSkipFaultyPipelines())
                log.warn(msg, e);
            else
                throw new PipelineRunnersFactoryException(msg, e);
        } catch (DatasetConsumerException e) {
            String msg = String.format("can't execute pipeline %s", pipelineName);
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
