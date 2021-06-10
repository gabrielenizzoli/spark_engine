package sparkengine.plan.model.mapper.parameters;

import lombok.Value;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.BatchComponent;
import sparkengine.plan.model.component.impl.InlineComponent;
import sparkengine.plan.model.component.impl.StreamComponent;
import sparkengine.plan.model.component.mapper.ComponentMapper;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

        if (component.getData() == null) {
            return component;
        }

        var replacementKeyParser = new ReplacementKeyParser(prefix, postfix);
        var replacementTemplateParser = new ReplacementTemplateParser(prefix, postfix);

        var newData = new ArrayList<Map<String, Object>>(component.getData().size());
        for (var row : component.getData()) {
            var newRow = new HashMap<>(row);
            for (var e : row.entrySet()) {

                // attempt key replacement
                var replacementKey = replacementKeyParser.getReplacementKey(e.getValue());
                if (replacementKey != null) {
                    var replacementValue = replacementKey.getValueOrDefault(parameters.get(replacementKey.getName()));
                    newRow.replace(e.getKey(), replacementValue);
                    continue;
                }

                // attempt template replacement
                var template = e.getValue() instanceof CharSequence ? (CharSequence)e.getValue(): null;
                if (template != null) {
                    var replacementTemplate = replacementTemplateParser.replaceTemplate(template.toString(), parameters);
                    newRow.replace(e.getKey(), replacementTemplate);
                }

            }
            newData.add(newRow);
        }

        return component.withData(newData);
    }

    @Override
    public Component mapBatchComponent(Location location, BatchComponent component) throws Exception {

        if (component == null || component.getOptions() == null || component.getOptions().isEmpty()) {
            return component;
        }

        var options = component.getOptions();
        HashMap<String, String> newOptions = ReplacementTemplateParser.mapOptions(options, prefix, postfix, parameters);

        return component.withOptions(newOptions);
    }

    @Override
    public Component mapStreamComponent(Location location, StreamComponent component) throws Exception {
        if (component == null || component.getOptions() == null || component.getOptions().isEmpty()) {
            return component;
        }

        var options = component.getOptions();
        HashMap<String, String> newOptions = ReplacementTemplateParser.mapOptions(options, prefix, postfix, parameters);

        return component.withOptions(newOptions);
    }

}
