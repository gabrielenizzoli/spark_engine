package sparkengine.plan.app;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import sparkengine.plan.model.builder.InputStreamSupplier;
import sparkengine.plan.model.builder.ModelFactories;
import sparkengine.plan.runtime.PipelineName;
import sparkengine.plan.runtime.PlanFactory;
import sparkengine.plan.runtime.PlanFactoryException;
import sparkengine.plan.runtime.builder.ModelPlanFactory;
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

    public void run() throws IOException {
        var planInputStream = getPlanInputStreamSupplier();
        var planFactory = getPlanFactory(planInputStream);
        executePlan(planFactory);
        waitOnSpark();
    }

    @Nonnull
    private InputStreamSupplier getPlanInputStreamSupplier() {
        return () -> {
            log.info("loading execution plan stream from " + runtimeArgs.getPlanLocation() + " ...");
            var conf = new Configuration();
            var fileSystem = FileSystem.get(conf);
            var executionPlanFile = new Path(runtimeArgs.getPlanLocation());
            log.info("fully qualified plan location " + fileSystem.makeQualified(executionPlanFile));
            return fileSystem.open(executionPlanFile);
        };
    }

    private PlanFactory getPlanFactory(InputStreamSupplier inputStreamSupplier)
            throws IOException {
        var plan = ModelFactories.readPlanFromYaml(inputStreamSupplier);
        log.trace("application plan: " + plan);
        return ModelPlanFactory.ofPlan(sparkSession, plan);
    }

    private void executePlan(PlanFactory planFactory) throws IOException {
        log.info("found pipelines " + planFactory.getPipelineNames());
        Consumer<PipelineName> runPipeline = pipelineName -> runPipeline(planFactory, pipelineName);
        if (runtimeArgs.isParallelPipelineExecution()) {
            log.info("running pipelines in parallel");
            planFactory.getPipelineNames().parallelStream().forEach(runPipeline::accept);
        } else {
            planFactory.getPipelineNames().forEach(runPipeline::accept);
        }
    }

    @SneakyThrows
    private void runPipeline(PlanFactory planFactory,
                             PipelineName pipelineName) {
        log.info("running pipeline " + pipelineName);
        try {
            var pipeline = planFactory.buildPipelineRunner(pipelineName);
            pipeline.run();
        } catch (PlanFactoryException e) {
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
