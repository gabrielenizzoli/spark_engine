package sparkengine.plan.app;

import com.beust.jcommander.JCommander;
import lombok.extern.log4j.Log4j;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import sparkengine.plan.app.runner.PlanRunner;
import sparkengine.plan.app.runner.RuntimeArgs;
import sparkengine.plan.model.builder.ModelFormatException;
import sparkengine.plan.model.resolver.PlanResolverException;
import sparkengine.plan.runtime.PipelineRunnersFactoryException;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;

import javax.annotation.Nonnull;
import java.io.IOException;

@Log4j
public class Start {

    static final ApplicationArgs APPLICATION_ARGS = new ApplicationArgs();
    static final RuntimeArgs RUNTIME_ARGS = new RuntimeArgs();

    public static void main(String[] args) throws
            IOException,
            PlanResolverException,
            PipelineRunnersFactoryException,
            DatasetConsumerException, ModelFormatException {

        var jcmd = initArguments(args);
        if (APPLICATION_ARGS.isHelp()) {
            jcmd.usage();
        } else {
            log.trace("application starting");
            try (var sparkSession = initializeSpark()) {
                PlanRunner.builder()
                        .log(log)
                        .sparkSession(sparkSession)
                        .runtimeArgs(RUNTIME_ARGS)
                        .build()
                        .run();
            }
            log.trace("application exiting");
        }


    }

    @Nonnull
    private static JCommander initArguments(String[] args) {
        var jcmd = JCommander.newBuilder().addObject(new Object[]{APPLICATION_ARGS, RUNTIME_ARGS}).build();
        jcmd.parse(args);
        log.setLevel(Level.toLevel(APPLICATION_ARGS.getLogLevel()));
        log.info("loaded application arguments " + APPLICATION_ARGS);
        log.info("loaded runtime arguments " + RUNTIME_ARGS);
        return jcmd;
    }

    private static SparkSession initializeSpark() throws IOException {
        if (SparkSession.getActiveSession().isDefined()) {
            throw new IOException("spark session already defined");
        }
        return SparkSession.builder().getOrCreate();
    }


}
