package sparkengine.plan.model.mapper.parameters;

import lombok.NonNull;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.regex.Pattern;

@Value
public class ReplacementKey {

    @NonNull
    String name;

    @NonNull
    ReplacementType replacementType;

    @Nullable
    Object defaultValue;

    @Nonnull
    public Object getValueOrDefault(String parameterValue) {
        if (parameterValue == null) {
            if (defaultValue != null) {
                return defaultValue;
            }
            throw new NullPointerException(String.format("parameter %s required but no replacement or null default value provided", name));
        }
        return Objects.requireNonNull(replacementType.convert(parameterValue), String.format("parameter %s is null in parameters map", name));
    }

}
