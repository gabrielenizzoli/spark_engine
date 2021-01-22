package sparkengine.plan.model.builder.input;

import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Value(staticConstructor = "of")
public class HttpInputStreamSupplier implements InputStreamSupplier {

    @Nonnull
    HttpRequest httpRequest;

    public static HttpInputStreamSupplier ofURI(URI uri, Duration timeout) {
        return of(HttpRequest.newBuilder().GET().uri(uri).timeout(timeout).build());
    }

    @Override
    public InputStream getInputStream() throws IOException {
        try {
            return HttpClient.newHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofInputStream()).body();
        } catch (IOException | InterruptedException e) {
            throw new IOException("failure in reading http url with io supplier [" + this + "]", e);
        }
    }

}
