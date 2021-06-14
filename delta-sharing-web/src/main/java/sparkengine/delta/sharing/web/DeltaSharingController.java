package sparkengine.delta.sharing.web;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.net.util.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import sparkengine.delta.sharing.protocol.v1.*;
import sparkengine.delta.sharing.web.store.ConfigRepository;
import sparkengine.delta.sharing.web.store.Page;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/delta-sharing/v1")
@Value
@AllArgsConstructor(onConstructor_ = {@Autowired})
public class DeltaSharingController {

    ConfigRepository configRepository;

    @GetMapping("/shares")
    Shares getShares(@RequestParam(value = "maxResults", defaultValue = "500") int maxResults,
                     @RequestParam(value = "nextPageToken") Optional<String> nextPageToken) {

        var page = getPage(maxResults, nextPageToken);
        var items = configRepository
                .streamShareNames(Optional.of(page))
                .map(name -> Share.builder().withName(name).build())
                .collect(Collectors.toList());
        var nextPage = items.size() < page.getSize() ? null : pageToToken(page);
        return Shares.builder().withItems(items).withNextPageToken(nextPage).build();
    }

    @GetMapping("/shares/{share}/schemas")
    Schemas getSchemas(
            @PathVariable("share") String shareName,
            @RequestParam(value = "maxResults", defaultValue = "500") int maxResults,
            @RequestParam(value = "nextPageToken") Optional<String> nextPageToken) {

        var page = getPage(maxResults, nextPageToken);

        var items = configRepository
                .streamSchemaNames(shareName, Optional.of(page))
                .map(name -> Schema.builder().withShare(shareName).withName(name).build())
                .collect(Collectors.toList());
        var nextPage = items.size() < page.getSize() ? null : pageToToken(page);
        return Schemas.builder().withItems(items).withNextPageToken(nextPage).build();
    }

    @GetMapping("/shares/{share}/schemas/{schema}/tables")
    Tables getTables(
            @PathVariable("share") String shareName,
            @PathVariable("schema") String schemaName,
            @RequestParam(value = "maxResults", defaultValue = "500") int maxResults,
            @RequestParam(value = "nextPageToken") Optional<String> nextPageToken) {

        var page = getPage(maxResults, nextPageToken);

        var items = configRepository
                .streamTableNames(shareName, schemaName, Optional.of(page))
                .map(name -> Table.builder().withShare(shareName).withSchema(schemaName).withName(name).build())
                .collect(Collectors.toList());
        var nextPage = items.size() < page.getSize() ? null : pageToToken(page);
        return Tables.builder().withItems(items).withNextPageToken(nextPage).build();
    }

    @Nonnull
    private static Page getPage(int maxResults, Optional<String> nextPageToken) {

        var page = new Page(tokenToStart(nextPageToken), maxResults);

        if (page.getSize() <= 0) {
            throw new IllegalArgumentException("request size is negative or zero");
        }
        if (page.getStart() < 0) {
            throw new IllegalArgumentException("start offset is negative");
        }
        return page;
    }

    private static int tokenToStart(Optional<String> nextPageToken) {
        return nextPageToken
                .map(token -> new String(Base64.decodeBase64(token), StandardCharsets.UTF_8))
                .map(Integer::parseInt)
                .orElse(0);
    }

    @Nonnull
    private static String pageToToken(Page page) {
        return Base64.encodeBase64String(Integer.toString(page.getStart() + page.getSize()).getBytes(StandardCharsets.UTF_8));
    }

}