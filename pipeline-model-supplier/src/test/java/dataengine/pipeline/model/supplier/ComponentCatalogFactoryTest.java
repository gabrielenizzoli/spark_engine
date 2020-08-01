package dataengine.pipeline.model.supplier;

import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.pipeline.model.description.source.ComponentCatalogException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

class ComponentCatalogFactoryTest {

    @Test
    void testYaml() throws IOException, ComponentCatalogException {

        File yamlSource = new File("src/test/resources/components.yaml");
        ComponentCatalog catalog = ComponentCatalogFactory.readYamlMap(() -> {
            try {
                return new FileInputStream(yamlSource);
            } catch (FileNotFoundException e) {
                throw new IllegalStateException();
            }
        });

        Optional<Component> sourceComponent = catalog.lookup("tx");

        Assertions.assertNotNull(sourceComponent);
        Assertions.assertTrue(sourceComponent.isPresent());

    }

}