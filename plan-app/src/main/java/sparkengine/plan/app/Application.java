package sparkengine.plan.app;

import com.beust.jcommander.JCommander;
import sparkengine.plan.model.builder.InputStreamSupplier;
import sparkengine.plan.model.builder.ModelFactories;
import sparkengine.plan.runtime.builder.ModelPlanFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.plan.runtime.PipelineName;
import sparkengine.plan.runtime.PlanFactory;
import sparkengine.plan.runtime.PlanFactoryException;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.function.Consumer;

@Log4j
public class Application {

    public static void main(String[] args) throws IOException {
        log.trace("application starting");

        var appArgs = getApplicationArguments(args);
        try (var sparkSession = initializeSpark()) {
            var planInputStream = getPlanInputStreamSupplier(appArgs);
            var planFactory = getPlanFactory(sparkSession, planInputStream);
            executePlan(planFactory, appArgs);
            waitOnSpark(sparkSession);
        }

        log.trace("application exiting");
    }

    @Nonnull
    private static ApplicationArgs getApplicationArguments(String[] args) {
        var appArgs = new ApplicationArgs();
        var jcmd = JCommander.newBuilder().addObject(appArgs).build();
        jcmd.parse(args);
        log.setLevel(Level.toLevel(appArgs.getLogLevel()));
        log.trace("loaded application arguments " + appArgs);
        if (appArgs.isHelp()) {
            jcmd.usage();
            System.exit(0);
        }
        return appArgs;
    }

    private static SparkSession initializeSpark() throws IOException {
        if (SparkSession.getActiveSession().isDefined()) {
            throw new IOException("spark session already defined");
        }
        return SparkSession.builder().getOrCreate();
    }

    @Nonnull
    private static InputStreamSupplier getPlanInputStreamSupplier(ApplicationArgs appArgs) {
        return () -> {
            log.info("loading execution plan stream from " + appArgs.getPlanLocation() + " ...");
            var conf = new Configuration();
            var fileSystem = FileSystem.get(conf);
            var executionPlanFile = new Path(appArgs.getPlanLocation());
            log.info("fully qualified plan location " + fileSystem.makeQualified(executionPlanFile));
            return fileSystem.open(executionPlanFile);
        };
    }

    private static PlanFactory getPlanFactory(SparkSession sparkSession, InputStreamSupplier inputStreamSupplier)
            throws IOException {
        var plan = ModelFactories.readPlanFromYaml(inputStreamSupplier);
        log.trace("application plan: " + plan);
        return ModelPlanFactory.ofPlan(sparkSession, plan);
    }

    private static void executePlan(PlanFactory planFactory, ApplicationArgs appArgs) throws IOException {
        log.info("found pipelines " + planFactory.getPipelineNames());
        Consumer<PipelineName> runPipeline = pipelineName -> runPipeline(planFactory, pipelineName, appArgs);
        if (appArgs.isParallelPipelineExecution()) {
            log.info("running pipelines in parallel");
            planFactory.getPipelineNames().parallelStream().forEach(runPipeline::accept);
        } else {
            planFactory.getPipelineNames().forEach(runPipeline::accept);
        }
    }

    @SneakyThrows
    private static void runPipeline(PlanFactory planFactory,
                                    PipelineName pipelineName,
                                    ApplicationArgs appArgs) {
        log.info("running pipeline " + pipelineName);
        try {
            var pipeline = planFactory.buildPipelineRunner(pipelineName);
            pipeline.run();
        } catch (PlanFactoryException e) {
            String msg = "can't instantiate pipeline " + pipelineName;
            if (appArgs.isSkipFaultyPipelines())
                log.warn(msg, e);
            else
                throw new IOException(msg, e);
        } catch (DatasetConsumerException e) {
            String msg = "can't execute pipeline " + pipelineName;
            if (appArgs.isSkipFaultyPipelines())
                log.warn(msg, e);
            else
                throw new IOException(msg, e);
        }
    }

    private static void waitOnSpark(SparkSession sparkSession) throws IOException {
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
