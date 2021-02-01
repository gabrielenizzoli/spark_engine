package sparkengine.plan.app;

import com.beust.jcommander.JCommander;
import lombok.Getter;
import lombok.ToString;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import sparkengine.plan.app.runner.RuntimeArgs;

@ToString
public class CommandLine {

    @Getter
    private final ApplicationArgs applicationArgs = new ApplicationArgs();
    @Getter
    private final RuntimeArgs runtimeArgs = new RuntimeArgs();
    private final JCommander jCommander = JCommander.newBuilder().addObject(new Object[]{applicationArgs, runtimeArgs}).build();

    public static CommandLine parseArgs(String[] args) {
        return new CommandLine().parse(args);
    }

    public CommandLine parse(String[] args) {
        jCommander.parse(args);
        return this;
    }

    public void usage() {
        jCommander.usage();
    }

    public Logger configureLogger(CommandLine commandLine, Logger log) {
        log.setLevel(Level.toLevel(commandLine.getApplicationArgs().getLogLevel()));
        log.info(String.format("loaded application arguments [%s]", commandLine.getApplicationArgs()));
        log.info(String.format("loaded runtime arguments [%s]", commandLine.getRuntimeArgs()));
        return log;
    }

}
