package sparkengine.plan.model.mapper.reference;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.ModelFormatException;
import sparkengine.plan.model.builder.ResourceLocationBuilder;
import sparkengine.plan.model.builder.input.FileResourceLocator;
import sparkengine.plan.model.component.impl.WrapperComponent;
import sparkengine.plan.model.mapper.reference.ReferenceComponentMapper;
import sparkengine.plan.model.mapper.reference.ReferencePlanMapper;
import sparkengine.plan.model.plan.mapper.PlanMapperException;

import java.io.IOException;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

class ReferencePlanMapperTest {

    @Test
    void testReferenceMapper() throws IOException, ModelFormatException, PlanMapperException {

        // given
        var planLocation = "src/test/resources/referenceMapperTest/nestedPlan.yaml";
        var resourceLocator = new FileResourceLocator();
        var plan = ModelFactory.readPlanFromYaml(resourceLocator.getInputStreamFactory(planLocation));
        var resourceLocationBuilder = new ResourceLocationBuilder(planLocation, "_", "yaml");
        var planMapper = ReferencePlanMapper.of(resourceLocationBuilder, resourceLocator);

        // when
        var resolvedPlan = planMapper.map(plan);

        // then
        var expectedPlanLocation = "src/test/resources/referenceMapperTest/nestedPlanExpected.yaml";
        var expectedPlan = ModelFactory.readPlanFromYaml(resourceLocator.getInputStreamFactory(expectedPlanLocation));
        assertEquals(expectedPlan, resolvedPlan);

    }

    @Test
    void testReferenceMapperWithFailure() throws IOException, ModelFormatException {

        // given
        var planLocation = "src/test/resources/referenceMapperTest/failedPlan.yaml";
        var resourceLocator = new FileResourceLocator();
        var plan = ModelFactory.readPlanFromYaml(resourceLocator.getInputStreamFactory(planLocation));
        var resourceLocationBuilder = new ResourceLocationBuilder(planLocation, "_", "yaml");
        var referenceComponentMapper = ReferenceComponentMapper.of(resourceLocationBuilder, resourceLocator);
        var planMapper = ReferencePlanMapper.of(resourceLocationBuilder, resourceLocator);

        // when
        assertThrows(PlanMapperException.class, () -> planMapper.map(plan));

    }
}