package sparkengine.plan.model.mapper.parameters;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class ReplacementKeyParser {

    private final Pattern keyPattern;

    public ReplacementKeyParser(String prefix, String postfix) {
        var regexp = "^" + Pattern.quote(prefix) + "([sbn]:|)([a-zA-Z0-9_-]+)(|:.*?)" + Pattern.quote(postfix) + "$";
        this.keyPattern = Pattern.compile(regexp);
    }

    @Nullable
    public ReplacementKey getReplacementKey(Object keyCandidate) {
        if (!(keyCandidate instanceof CharSequence)) {
            return null;
        }
        var key = keyCandidate.toString();
        var matcher = keyPattern.matcher(key);
        if (!matcher.matches()) {
            return null;
        }

        var name = matcher.group(2);
        var replacementType = getReplacementType(matcher.group(1));
        var defaultValue = getDefaultValue(matcher.group(3), replacementType);

        return new ReplacementKey(name, replacementType, defaultValue);
    }

    @Nullable
    private Object getDefaultValue(String defaultStr, ReplacementType replacementType) {
        defaultStr = defaultStr.replaceAll("^:", "");
        return replacementType.convert(defaultStr);
    }

    @Nonnull
    private ReplacementType getReplacementType(String typeStr) {
        typeStr = typeStr.replaceAll(":$", "");
        var replacementType = ReplacementType.STRING;
        if (!typeStr.isBlank()) {
            switch (typeStr) {
                case "n": {
                    replacementType = ReplacementType.NUMBER;
                    break;
                }
                case "b": {
                    replacementType = ReplacementType.BOOLEAN;
                    break;
                }
                case "s": {
                    replacementType = ReplacementType.STRING;
                    break;
                }
                default: {
                    throw new IllegalArgumentException("unmanaged conversion type " + typeStr);
                }
            }
        }
        return replacementType;
    }

}
