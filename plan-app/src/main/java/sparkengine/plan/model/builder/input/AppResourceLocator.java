package sparkengine.plan.model.builder.input;

import java.net.URI;
import java.time.Duration;

public class AppResourceLocator extends BaseResourceLocator {

    @Override
    public InputStreamSupplier getInputStreamSupplier(String name) {
        var uri = URI.create(name);
        return getInputStreamSupplier(uri);
    }

}
