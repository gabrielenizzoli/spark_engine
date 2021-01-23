package sparkengine.plan.model.builder.input;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.*;

class URIBuilderTest {

    @Test
    void getFolder() throws URISyntaxException {

        // given
        var u = URIBuilder.ofString("http://domain.com/folder1/folder2/file.yaml");

        // when
        var u2 = u.removePartFromPath();

        // then
        assertEquals(URI.create("http://domain.com/folder1/folder2"), u2.getUri());
    }

    @Test
    void getFolderFromFolder() throws URISyntaxException {

        // given
        var u = URIBuilder.ofString("http://domain.com/folder1/folder2/");

        // when
        var u2 = u.removePartFromPath();

        // then
        assertEquals(URI.create("http://domain.com/folder1"), u2.getUri());
    }

    @Test
    void getFolderWithoutFolder() throws URISyntaxException {

        // given
        var u = URIBuilder.ofString("http://domain.com/");

        // when
        var u2 = u.removePartFromPath();

        // then
        assertEquals(URI.create("http://domain.com"), u2.getUri());
    }

    @Test
    void getFolderWithHdfs() throws URISyntaxException {

        // given
        var u = URIBuilder.ofString("hdfs://storage.server/folder1/folder2/file.yaml");

        // when
        var u2 = u.removePartFromPath();

        // then
        assertEquals(URI.create("hdfs://storage.server/folder1/folder2"), u2.getUri());
    }

    @Test
    void getFolderWithFile() throws URISyntaxException {

        // given
        var u = URIBuilder.ofString("file:///folder1/folder2/file.yaml");

        // when
        var u2 = u.removePartFromPath();

        // then
        assertEquals(URI.create("file:///folder1/folder2"), u2.getUri());
    }

    @Test
    void getFolderWithoutSchemeOrServer() throws URISyntaxException {

        // given
        var u = URIBuilder.ofString("/folder1/folder2/file.yaml");

        // when
        var u2 = u.removePartFromPath();

        // then
        assertEquals(URI.create("/folder1/folder2"), u2.getUri());
    }

    @Test
    void getFolderWithRelativeLocation() throws URISyntaxException {

        // given
        var u = URIBuilder.ofString("./folder1/folder2/file.yaml");

        // when
        var u2 = u.removePartFromPath();

        // then
        assertEquals(URI.create("./folder1/folder2"), u2.getUri());
    }

}