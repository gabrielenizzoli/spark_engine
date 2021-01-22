package sparkengine.plan.model.builder.input;

import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.net.URI;

@Value
@Builder
public class AbsoluteResourceLocator extends BaseResourceLocator {

    @Nonnull
    String extension;

    @Override
    public InputStreamSupplier getInputStreamSupplier(String name) {
        var uri = URIBuilder.ofString(name).addPartToPath(name + "." + extension).getUri();
        return getInputStreamSupplier(uri);
    }

}
