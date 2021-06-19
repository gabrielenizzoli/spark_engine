package sparkengine.delta.sharing.web;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.net.util.Base64;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import sparkengine.delta.sharing.model.TableName;
import sparkengine.delta.sharing.protocol.v1.*;
import sparkengine.delta.sharing.utils.TableLayoutLoaderException;
import sparkengine.delta.sharing.utils.TableLayoutStore;
import sparkengine.delta.sharing.web.store.ConfigRepository;
import sparkengine.delta.sharing.web.store.Page;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/delta-sharing/v1")
@Value
@AllArgsConstructor(onConstructor_ = {@Autowired})
public class DeltaSharingController {

    public static final String DELTA_TABLE_VERSION = "Delta-Table-Version";
    @Autowired
    ConfigRepository configRepository;

    @Autowired
    TableLayoutStore tableLayoutStore;

    private static final ObjectMapper OBJECT_MAPPER_DNJSON;

    static {
        OBJECT_MAPPER_DNJSON = new ObjectMapper();
        OBJECT_MAPPER_DNJSON.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
        OBJECT_MAPPER_DNJSON.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

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

    @RequestMapping(method = RequestMethod.HEAD, path = "/shares/{share}/schemas/{schema}/tables/{table}")
    public ResponseEntity<String> getTableVersion(@PathVariable("share") String shareName,
                                                  @PathVariable("schema") String schemaName,
                                                  @PathVariable("table") String tableName) throws TableLayoutLoaderException {


        var tableLayout = tableLayoutStore.getTableLayout(new TableName(shareName, schemaName, tableName), configRepository::getTableMetadata);

        return ResponseEntity.ok().header(DELTA_TABLE_VERSION, Long.toString(tableLayout.getTableVersion())).build();
    }

    @GetMapping(value = "/shares/{share}/schemas/{schema}/tables/{table}/metadata", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public ResponseEntity<ResponseBodyEmitter> getTableMetadata(@PathVariable("share") String shareName,
                                                                @PathVariable("schema") String schemaName,
                                                                @PathVariable("table") String tableName) throws TableLayoutLoaderException, IOException {

        var tableLayout = tableLayoutStore.getTableLayout(new TableName(shareName, schemaName, tableName), configRepository::getTableMetadata);

        ResponseBodyEmitter responseBodyEmitter = new ResponseBodyEmitter();
        try {
            for (var wrapper : tableLayout.streamTableMetadataProtocol().collect(Collectors.toList())) {
                responseBodyEmitter.send(OBJECT_MAPPER_DNJSON.writeValueAsString(wrapper));
                responseBodyEmitter.send("\n");
            }
        } finally {
            responseBodyEmitter.complete();
        }

        return ResponseEntity.ok()
                .header(DELTA_TABLE_VERSION, Long.toString(tableLayout.getTableVersion()))
                .contentType(MediaType.APPLICATION_NDJSON)
                .body(responseBodyEmitter);
    }

    @PostMapping(value = "/shares/{share}/schemas/{schema}/tables/{table}/query",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_NDJSON_VALUE)
    public ResponseEntity<ResponseBodyEmitter> getTableQuery(
            @RequestBody TableQuery tableQuery,
            @PathVariable("share") String shareName,
            @PathVariable("schema") String schemaName,
            @PathVariable("table") String tableName) throws TableLayoutLoaderException, IOException {

        var tableLayout = tableLayoutStore.getTableLayout(new TableName(shareName, schemaName, tableName), configRepository::getTableMetadata);

        ResponseBodyEmitter responseBodyEmitter = new ResponseBodyEmitter();
        try {
            for (var wrapper : tableLayout.streamTableProtocol(true, tableQuery.getPredicateHints()).collect(Collectors.toList())) {
                responseBodyEmitter.send(OBJECT_MAPPER_DNJSON.writeValueAsString(wrapper));
                responseBodyEmitter.send("\n");
            }
        } finally {
            responseBodyEmitter.complete();
        }

        return ResponseEntity.ok()
                .header(DELTA_TABLE_VERSION, Long.toString(tableLayout.getTableVersion()))
                .contentType(MediaType.APPLICATION_NDJSON)
                .body(responseBodyEmitter);
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