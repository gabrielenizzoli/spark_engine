package sparkengine.plan.model.builder.input;

import java.net.URI;
import java.time.Duration;

public abstract class BaseResourceLocator implements InputStreamResourceLocator {

    protected final InputStreamSupplier getInputStreamSupplier(URI uri) {
        switch (uri.getScheme().toLowerCase()) {
            case "http":
            case "https:":
                return HttpInputStreamSupplier.ofURI(uri, Duration.ofSeconds(30));
            default:
                return HdfsInputStreamSupplier.of(uri);
        }
    }

}
