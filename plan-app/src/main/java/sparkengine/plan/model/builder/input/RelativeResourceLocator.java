package sparkengine.plan.model.builder.input;

import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.net.URI;

@Value
@Builder
public class RelativeResourceLocator extends BaseResourceLocator {

    @Nonnull
    URI baseLocation;
    @Nonnull
    String extension;

    @Override
    public InputStreamFactory getInputStreamFactory(String name) {
        var uri = URIBuilder.of(baseLocation).addPartToPath(name + "." + extension).getUri();
        return getInputStreamFactory(uri);
    }

}
