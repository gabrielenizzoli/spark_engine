package sparkengine.plan.model.builder.input;

import java.net.URI;


public class AbsoluteResourceLocator extends BaseResourceLocator {

    @Override
    public InputStreamFactory getInputStreamFactory(String name) {
        var uri = URI.create(name);
        return getInputStreamFactory(uri);
    }

}
