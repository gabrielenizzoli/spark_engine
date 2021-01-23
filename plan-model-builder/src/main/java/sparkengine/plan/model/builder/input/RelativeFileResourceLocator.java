package sparkengine.plan.model.builder.input;

import lombok.Value;

import javax.annotation.Nonnull;
import java.io.FileInputStream;

@Value(staticConstructor = "of")
public class RelativeFileResourceLocator implements InputStreamResourceLocator {

    @Nonnull
    String base;
    @Nonnull
    String extension;

    @Override
    public InputStreamFactory getInputStreamFactory(String name) {
        return () -> new FileInputStream(base + name + "." + extension);
    }

}
