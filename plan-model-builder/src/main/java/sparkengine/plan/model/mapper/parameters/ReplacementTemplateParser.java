package sparkengine.plan.model.mapper.parameters;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ReplacementTemplateParser {

    private final Pattern templatePattern;

    public ReplacementTemplateParser(String prefix, String postfix) {
        var regexp = Pattern.quote(prefix) + "([a-zA-Z0-9_-]+)(|:.*?)" + Pattern.quote(postfix);
        this.templatePattern = Pattern.compile(regexp);
    }

    @Nonnull
    public static HashMap<String, String> mapOptions(Map<String, String> options,
                                                        String prefix,
                                                        String postfix,
                                                        Map<String, String> parameters) {
        var replacementTemplateParser = new ReplacementTemplateParser(prefix, postfix);
        var newOptions = new HashMap<String, String>();
        for (var e : options.entrySet()) {
            var template = e.getValue();
            var replacement = replacementTemplateParser.replaceTemplate(template, parameters);
            newOptions.put(e.getKey(), replacement);
        }
        return newOptions;
    }

    @Nullable
    public String replaceTemplate(String templateString, Map<String, String> parameters) {

        if (templateString == null || templateString.isBlank()) {
            return templateString;
        }

        return templatePattern
                .matcher(templateString)
                .replaceAll((matcher -> {
                    var name = matcher.group(1);
                    var defaultValue = getDefaultValue(matcher.group(2));
                    var value = parameters.getOrDefault(name, defaultValue);

                    if (value == null) {
                        throw new NullPointerException(String.format("parameter %s required but no replacement or null default value provided", name));
                    }

                    return value;
                }));
    }

    @Nullable
    private String getDefaultValue(String defaultStr) {
        defaultStr = defaultStr.replaceAll("^:", "");
        if (defaultStr.isBlank()) {
            return null;
        }
        return defaultStr;
    }


}
