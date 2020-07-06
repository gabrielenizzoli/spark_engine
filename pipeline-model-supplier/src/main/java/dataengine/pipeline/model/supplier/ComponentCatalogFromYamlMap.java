package dataengine.pipeline.model.supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.pipeline.model.description.source.ComponentCatalogException;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.NonFinal;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;

@Value
@Builder
public class ComponentCatalogFromYamlMap implements ComponentCatalog {

    @Nonnull
    Supplier<InputStream> inputStreamFactory;
    @NonFinal
    Map<String, Component> components = null;

    @Override
    public Component lookup(String componentName) throws ComponentCatalogException {
        if (components == null)
            read();
        return components.get(componentName);
    }

    public void read() throws ComponentCatalogException {
        try(InputStream inputStream = inputStreamFactory.get()) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            components = mapper.readValue(inputStream, new TypeReference<Map<String, Component>>() {
            });
        } catch (Exception e) {
            throw new ComponentCatalogException("can't build component", e);
        }
    }

}
