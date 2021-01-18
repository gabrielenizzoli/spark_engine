package sparkengine.plan.model.builder;

import java.io.IOException;
import java.io.InputStream;

public interface InputStreamSupplier {

    InputStream getInputStream() throws IOException;

}
