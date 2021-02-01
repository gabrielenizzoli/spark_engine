package sparkengine.plan.app;

import lombok.extern.log4j.Log4j;

@Log4j
public class Start {

    public static void main(String[] args) throws Throwable {

        var commandLine = CommandLine.parseArgs(args);

        if (commandLine.getApplicationArgs().isHelp()) {
            commandLine.usage();
        } else {
            Starter.builder()
                    .applicationArgs(commandLine.getApplicationArgs())
                    .runtimeArgs(commandLine.getRuntimeArgs())
                    .log(commandLine.configureLogger(commandLine, log))
                    .build()
                    .start();
        }

    }

}
