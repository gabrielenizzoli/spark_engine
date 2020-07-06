package datangine.pipeline.model.builder.source;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.pipeline.model.description.source.ComponentCatalogException;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Map;

public class Utils {

    @Nonnull
    public static ComponentCatalog getComponentCatalog() {
        return new ComponentCatalog() {

            private Map<String, Component> cachedSteps;

            @Override
            public Component lookup(String name) throws ComponentCatalogException {
                if (cachedSteps == null)
                    read();
                return cachedSteps.get(name);
            }

            public void read() throws ComponentCatalogException {
                try {
                    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                    File yamlSource = new File("src/test/resources/testComponentsCatalog.yaml");
                    cachedSteps = mapper.readValue(yamlSource, new TypeReference<Map<String, Component>>() {
                    });
                } catch (Exception e) {
                    throw new ComponentCatalogException("can't build", e);
                }
            }
        };
    }

}
