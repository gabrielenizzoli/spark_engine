package sparkengine.plan.model.builder.input;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

public abstract class BaseResourceLocator implements InputStreamResourceLocator {

    protected final InputStreamFactory getInputStreamFactory(URI uri) {
        switch (Optional.ofNullable(uri.getScheme()).orElse("")) {
            case "http":
            case "https:":
                return HttpInputStreamFactory.ofURI(uri, Duration.ofSeconds(30));
            default:
                return HdfsInputStreamFactory.of(uri);
        }
    }

}
