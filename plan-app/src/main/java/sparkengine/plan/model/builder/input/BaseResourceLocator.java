package sparkengine.plan.model.builder.input;

import java.net.URI;
import java.time.Duration;

public abstract class BaseResourceLocator implements InputStreamResourceLocator {

    protected final InputStreamFactory getInputStreamFactory(URI uri) {
        switch (uri.getScheme().toLowerCase()) {
            case "http":
            case "https:":
                return HttpInputStreamFactory.ofURI(uri, Duration.ofSeconds(30));
            default:
                return HdfsInputStreamFactory.of(uri);
        }
    }

}
