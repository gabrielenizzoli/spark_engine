package sparkengine.plan.model.builder.input;

import java.io.IOException;
import java.io.InputStream;

public interface InputStreamFactory {

    InputStream getInputStream() throws IOException;

}
