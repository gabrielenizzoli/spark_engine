package dataengine.pipeline.model.builder;

import java.io.IOException;
import java.io.InputStream;

public interface InputStreamSupplier {

    InputStream getInputStream() throws IOException;

}
