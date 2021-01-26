package sparkengine.plan.model.mapper.impl;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.builder.input.FileResourceLocator;
import sparkengine.plan.model.component.impl.ReferenceComponent;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.component.mapper.ComponentsMapper;
import sparkengine.plan.model.mapper.ComponentResourceLocationBuilder;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReferenceComponentMapperTest {

    @Test
    void testRelativeReference() throws Exception {

        // given
        var resourceLocationBuilder = new ComponentResourceLocationBuilder("src/test/resources/referenceComponentMapperTest/plan.yaml", "_", "yaml");
        var referenceComponentMapper = ReferenceComponentMapper.of(resourceLocationBuilder, new FileResourceLocator());
        var referenceComponent = ReferenceComponent.builder().withMode(ReferenceComponent.ReferenceMode.RELATIVE).build();
        var location = ComponentsMapper.locationOf("location");

        // when
        var referencedComponent = referenceComponentMapper.mapReferenceComponent(location, referenceComponent);

        // then
        assertEquals(SqlComponent.TYPE_NAME, referencedComponent.componentTypeName());
    }

    @Test
    void testRelativeReferenceWithMultipleLocations() throws Exception {

        // given
        var resourceLocationBuilder = new ComponentResourceLocationBuilder("src/test/resources/referenceComponentMapperTest/plan.yaml", "_", "yaml");
        var referenceComponentMapper = ReferenceComponentMapper.of(resourceLocationBuilder, new FileResourceLocator());
        var referenceComponent = ReferenceComponent.builder().withMode(ReferenceComponent.ReferenceMode.RELATIVE).build();
        var location = ComponentsMapper.locationOf("location1", "location2");

        // when
        var referencedComponent = referenceComponentMapper.mapReferenceComponent(location, referenceComponent);

        // then
        assertEquals(SqlComponent.TYPE_NAME, referencedComponent.componentTypeName());
    }

    @Test
    void testRelativeReferenceWithMissingLocations() throws Exception {

        // given
        var resourceLocationBuilder = new ComponentResourceLocationBuilder("src/test/resources/referenceComponentMapperTest/plan.yaml", "_", "yaml");
        var referenceComponentMapper = ReferenceComponentMapper.of(resourceLocationBuilder, new FileResourceLocator());
        var referenceComponent = ReferenceComponent.builder().withMode(ReferenceComponent.ReferenceMode.RELATIVE).build();
        var location = ComponentsMapper.locationOf("location2", "location1");

        // then
        assertThrows(IOException.class, () -> referenceComponentMapper.mapReferenceComponent(location, referenceComponent));
    }

    @Test
    void testAbsoluteReference() throws Exception {

        // given
        var resourceLocationBuilder = new ComponentResourceLocationBuilder("src/test/resources/referenceComponentMapperTest/plan_location.yaml", "_", "yaml");
        var referenceComponentMapper = ReferenceComponentMapper.of(resourceLocationBuilder, new FileResourceLocator());
        var referenceComponent = ReferenceComponent.builder()
                .withMode(ReferenceComponent.ReferenceMode.ABSOLUTE)
                .withRef("src/test/resources/referenceComponentMapperTest/plan_location.yaml")
                .build();
        var location = ComponentsMapper.locationOf("another-location");

        // when
        var referencedComponent = referenceComponentMapper.mapReferenceComponent(location, referenceComponent);

        // then
        assertEquals(SqlComponent.TYPE_NAME, referencedComponent.componentTypeName());
    }

}