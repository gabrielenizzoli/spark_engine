package sparkengine.plan.model.builder.input;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;


public class AppResourceLocator implements InputStreamResourceLocator {

    @Override
    public InputStreamFactory getInputStreamFactory(String name) {
        var uri = URI.create(name);
        return getInputStreamFactory(uri);
    }

    private InputStreamFactory getInputStreamFactory(URI uri) {
        var scheme = Optional.ofNullable(uri.getScheme()).orElse("");
        switch (scheme) {
            case "http":
            case "https":
                return HttpInputStreamFactory.ofURI(uri, Duration.ofSeconds(30));
            default:
                return HdfsInputStreamFactory.of(uri);
        }
    }

}
