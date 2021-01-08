package dataengine.pipeline.model.reader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.component.catalog.ComponentCatalogFromMap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;

public class ComponentCatalogReader {

    public static ComponentCatalog readYamlMap(@Nonnull Supplier<InputStream> inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.get()) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Map<String, Component> map = mapper.readValue(inputStream, new TypeReference<Map<String, Component>>() {
            });
            return ComponentCatalogFromMap.of(map);
        }
    }

}
