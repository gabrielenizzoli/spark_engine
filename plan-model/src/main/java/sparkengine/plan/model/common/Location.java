package sparkengine.plan.model.common;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@ToString
@EqualsAndHashCode
public class Location {

    private final List<String> location;

    private Location(List<String> location) {
        this.location = location.stream()
                .peek(Objects::requireNonNull)
                .map(String::strip)
                .collect(Collectors.toList());
    }

    public static Location empty() {
        return new Location(List.of());
    }

    public static Location of(String... parts) {
        return new Location(List.of(parts));
    }

    public boolean isEmpty() {
        return location.isEmpty();
    }

    public String joinWith(@Nonnull String joinString) {
        Objects.requireNonNull(joinString);
        return String.join(joinString.strip(), location);
    }

    public Location push(@Nonnull String value) {
        Objects.requireNonNull(value);
        List<String> newLocation = new ArrayList<>(location);
        newLocation.add(value.strip());
        return new Location(newLocation);
    }

}
