package sparkengine.plan.app;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import lombok.*;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.regex.Pattern;

@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApplicationArgs {

    @Getter
    @Parameter(names = {"-h", "--help"}, help = true, required = false, order = 1, description = "Help usage")
    @lombok.Builder.Default
    private boolean help = false;

    @Getter
    @Parameter(names = {"-p", "--plan"}, required = false, order = 1, description = "The location of the plan. If missing, the source will be the standard input.")
    private String planLocation;

    @Getter
    @Parameter(names = {"-l", "--log"}, description = "Set main application log level (one of OFF,FATAL,ERROR,WARN,INFO,DEBUG,TRACE,ALL)")
    @lombok.Builder.Default
    private String logLevel = "INFO";

    @Getter
    @Parameter(names = {"--skipStackTrace"}, description = "Skip full stackTrace when printing application errors")
    @lombok.Builder.Default
    private boolean skipStackTrace = false;

    @Getter
    @Parameter(names = {"--sparkSessionReuse"}, description = "Reuse spark session if already defined")
    @lombok.Builder.Default
    private boolean sparkSessionReuse = false;

}
