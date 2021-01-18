package sparkengine.plan.model.builder;

import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.component.catalog.ComponentCatalogException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

class ModelFactoriesTest {

    @Test
    void testYaml() throws IOException, ComponentCatalogException {

        File yamlSource = new File("src/test/resources/components.yaml");
        ComponentCatalog catalog = ModelFactories.readComponentMapFromYaml(() -> {
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