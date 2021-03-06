package sparkengine.plan.model.builder.input;

import java.io.FileInputStream;

public class FileResourceLocator implements InputStreamResourceLocator {

    @Override
    public InputStreamFactory getInputStreamFactory(String name) {
        return () -> new FileInputStream(name);
    }

}
