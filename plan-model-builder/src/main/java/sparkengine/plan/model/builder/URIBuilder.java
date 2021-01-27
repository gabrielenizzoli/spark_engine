package sparkengine.plan.model.builder;

import lombok.Value;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;

@Value(staticConstructor = "of")
public class URIBuilder {

    URI uri;

    public static URIBuilder ofString(String str) {
        return URIBuilder.of(URI.create(str));
    }

    public String getLastPartFromPath() {
        return uri.getPath().replaceAll("/$", "").replaceAll("^.*/", "");
    }

    public URIBuilder removePartFromPath() {
        String path = uri.getPath().replaceAll("/$", "").replaceAll("/[^/]+?$", "");
        return URIBuilder.of(withPath(path));
    }

    public URIBuilder addToPath(String name) {
        String path = uri.getPath().replaceAll("/$", "") + name;
        return URIBuilder.of(withPath(path));
    }

    public URIBuilder addPartToPath(String name) {
        String path = uri.getPath().replaceAll("/$", "") + "/" + name;
        return URIBuilder.of(withPath(path));
    }

    @Nonnull
    private URI withPath(String path) {
        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), path, uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("can't build url from uri [" + uri + "] with new path [" + path + "]", e);
        }
    }

    public String toString() {
        return uri.toString();
    }

}
