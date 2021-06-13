package sparkengine.delta.sharing.web;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sparkengine.delta.sharing.protocol.v1.Share;
import sparkengine.delta.sharing.protocol.v1.Shares;
import sparkengine.delta.sharing.web.store.ConfigRepository;
import sparkengine.delta.sharing.web.store.Page;

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

        var page = new Page(0, nextPageToken.map(Integer::parseInt).orElse(0));

        if (page.getSize() <= 0) {
            throw new IllegalArgumentException("request size is negative or zero");
        }
        if (page.getStart() < 0) {
            throw new IllegalArgumentException("start offset is negative");
        }

        var items = configRepository.streamAllShareNames(Optional.of(page)).map(name -> Share.builder().withName(name).build()).collect(Collectors.toList());
        var nextPage = items.size() < page.getSize() ? null : Integer.toString(page.getStart()+page.getStart());
        return Shares.builder().withItems(items).withNextPageToken(nextPage).build();
    }

}