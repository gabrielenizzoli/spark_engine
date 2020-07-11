package datangine.pipeline.model.builder.source;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.pipeline.model.description.source.ComponentCatalogException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Map;
import java.util.Optional;

public class TestUtils {

    @Nonnull
    public static ComponentCatalog getComponentCatalog(@Nullable String resourceName) {
        return new ComponentCatalog() {

            private Map<String, Component> cachedSteps;

            @Override
            @Nonnull
            public Optional<Component> lookup(String name) throws ComponentCatalogException {
                if (cachedSteps == null)
                    read();
                return Optional.ofNullable(cachedSteps.get(name));
            }

            public void read() throws ComponentCatalogException {
                try {
                    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                    File yamlSource = new File("src/test/resources/" + Optional.ofNullable(resourceName).orElse("testComponentsCatalog") + ".yaml");
                    cachedSteps = mapper.readValue(yamlSource, new TypeReference<Map<String, Component>>() {
                    });
                } catch (Exception e) {
                    throw new ComponentCatalogException("can't build", e);
                }
            }
        };
    }

}
