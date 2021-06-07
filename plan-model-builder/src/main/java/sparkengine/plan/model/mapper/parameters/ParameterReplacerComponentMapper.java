package sparkengine.plan.model.mapper.parameters;

import lombok.Value;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.InlineComponent;
import sparkengine.plan.model.component.mapper.ComponentMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Value(staticConstructor = "of")
public class ParameterReplacerComponentMapper implements ComponentMapper {

    @Nonnull
    Map<String, String> parameters;
    @Nonnull
    String prefix;
    @Nonnull
    String postfix;

    @Override
    public Component mapInlineComponent(Location location, InlineComponent component) throws Exception {

        var replacementKeyParser = new ReplacementKeyParser(prefix, postfix);

        if (component.getData() == null) {
            return component;
        }

        var newData = new ArrayList<Map<String, Object>>(component.getData().size());
        for (var row : component.getData()) {
            var newRow = new HashMap<>(row);
            for (var e : row.entrySet()) {
                var replacementKey = replacementKeyParser.getReplacementKey(e.getValue());
                if (replacementKey == null) {
                    continue;
                }
                var replacementValue = replacementKey.getValueOrDefault(parameters.get(replacementKey.getName()));
                newRow.replace(e.getKey(), replacementValue);
            }
            newData.add(newRow);
        }

        return component.withData(newData);
    }

}
