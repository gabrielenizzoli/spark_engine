package sparkengine.plan.app;

import lombok.Builder;
import lombok.Value;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import sparkengine.plan.app.runner.PlanRunner;
import sparkengine.plan.app.runner.RuntimeArgs;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;

@Value
@Builder
public class Starter {

    @Nonnull
    @lombok.Builder.Default
    ApplicationArgs applicationArgs = new ApplicationArgs();
    @Nonnull
    @lombok.Builder.Default
    RuntimeArgs runtimeArgs = new RuntimeArgs();
    @Nonnull
    @lombok.Builder.Default
    Logger log = Logger.getLogger(Starter.class);

    public void start() throws Throwable {


        log.info("START ====================================================================");

        try (var sparkSessionHolder = initializeSpark()) {
            PlanRunner.builder()
                    .log(log)
                    .sparkSession(sparkSessionHolder.getSparkSession())
                    .runtimeArgs(runtimeArgs)
                    .build()
                    .run();
        } catch (Throwable t) {
            if (applicationArgs.isSkipStackTrace()) {
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

    @Value
    private static class SparkSessionHolder implements Closeable {
        @Nonnull
        SparkSession sparkSession;
        boolean closeOnExit;

        @Override
        public void close() throws IOException {
            if (closeOnExit)
                sparkSession.close();
        }

    }

    @Nonnull
    private SparkSessionHolder initializeSpark() throws IOException {
        var opt = SparkSession.getActiveSession();
        SparkSession sparkSession = opt.isDefined() ? opt.get() : null;
        var closeOnExit = true;

        if (sparkSession != null && !applicationArgs.isSparkSessionReuse()) {
            throw new IOException("spark session already defined and starter will not reuse it");
        }

        if (sparkSession == null) {
            sparkSession = SparkSession.builder().getOrCreate();
        } else {
            closeOnExit = false;
        }

        return new SparkSessionHolder(sparkSession, closeOnExit);
    }

}
