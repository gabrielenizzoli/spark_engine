package sparkengine.plan.app;

import com.beust.jcommander.JCommander;
import lombok.extern.log4j.Log4j;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import sparkengine.plan.app.runner.PlanRunner;
import sparkengine.plan.app.runner.RuntimeArgs;

import javax.annotation.Nonnull;
import java.io.IOException;

@Log4j
public class Start {

    static final ApplicationArgs APPLICATION_ARGS = new ApplicationArgs();
    static final RuntimeArgs RUNTIME_ARGS = new RuntimeArgs();

    public static void main(String[] args) throws Throwable {

        var commandLine = initArguments(args);

        log.info("START ====================================================================");

        try {
            if (APPLICATION_ARGS.isHelp()) {
                commandLine.usage();
            } else {
                try (var sparkSession = initializeSpark()) {
                    PlanRunner.builder()
                            .log(log)
                            .sparkSession(sparkSession)
                            .runtimeArgs(RUNTIME_ARGS)
                            .build()
                            .run();
                }
            }
        } catch (Throwable t) {
            if (APPLICATION_ARGS.isSkipStackTrace()) {
                for (var error = t; error != null; error = error.getCause()) {
                    log.error(String.format("%s: %s", error.getClass().getSimpleName(), error.getMessage()));
                }
            } else {
                throw t;
            }
        } finally {
            log.info("STOP =====================================================================");
        }

    }

    @Nonnull
    private static JCommander initArguments(String[] args) {
        var jcmd = JCommander.newBuilder().addObject(new Object[]{APPLICATION_ARGS, RUNTIME_ARGS}).build();
        jcmd.parse(args);
        log.setLevel(Level.toLevel(APPLICATION_ARGS.getLogLevel()));
        log.info(String.format("loaded application arguments [%s]", APPLICATION_ARGS));
        log.info(String.format("loaded runtime arguments [%s]", RUNTIME_ARGS));
        return jcmd;
    }

    private static SparkSession initializeSpark() throws IOException {
        if (SparkSession.getActiveSession().isDefined()) {
            throw new IOException("spark session already defined");
        }
        return SparkSession.builder().getOrCreate();
    }


}
