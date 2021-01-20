package sparkengine.plan.app;

import com.beust.jcommander.Parameter;
import lombok.Getter;

public class ApplicationArgs {

    @Getter
    @Parameter(names = {"-h", "--help"}, help = true, required = false, order = 1, description = "Help usage")
    private boolean help = false;

    @Getter
    @Parameter(names = {"-l", "--log"}, order = 1, description = "Set main application log level (one of OFF,FATAL,ERROR,WARN,INFO,DEBUG,TRACE,ALL)")
    private String logLevel = "INFO";

}
