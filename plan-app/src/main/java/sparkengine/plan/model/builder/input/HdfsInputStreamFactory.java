package sparkengine.plan.model.builder.input;

import lombok.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

@Value(staticConstructor = "of")
public class HdfsInputStreamFactory implements InputStreamFactory {

    @Nonnull
    URI location;

    @Override
    public InputStream getInputStream() throws IOException {
        var conf = new Configuration();
        var fileSystem = FileSystem.get(conf);
        var executionPlanFile = new Path(location);
        return fileSystem.open(executionPlanFile);
    }

}
