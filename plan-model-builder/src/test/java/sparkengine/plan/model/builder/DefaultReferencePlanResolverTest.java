package sparkengine.plan.model.builder;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.builder.input.AbsoluteFileResourceLocator;
import sparkengine.plan.model.builder.input.RelativeFileResourceLocator;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.component.impl.ReferenceComponent;
import sparkengine.plan.model.component.impl.SqlComponent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class DefaultReferencePlanResolverTest {

    @Test
    void resolve() throws IOException, PlanResolverException {

        // given
        var resolver = DefaultReferencePlanResolver.builder()
                .absoluteResourceLocator(new AbsoluteFileResourceLocator())
                .relativeResourceLocator(RelativeFileResourceLocator.of("src/test/resources/planResolverTest/plan_", "yaml"))
                .build();
        var plan = ModelFactory.readPlanFromYaml(() -> new FileInputStream("src/test/resources/planResolverTest/plan.yaml"));

        // when
        var newPlan = resolver.resolve(plan);

        // then
        assertEquals(ReferenceComponent.TYPE_NAME, plan.getComponents().get("reference").componentTypeName());
        assertEquals(SqlComponent.TYPE_NAME, newPlan.getComponents().get("reference").componentTypeName());

        assertEquals(ReferenceComponent.TYPE_NAME, plan.getComponents().get("reference2").componentTypeName());
        assertEquals(SqlComponent.TYPE_NAME, newPlan.getComponents().get("reference2").componentTypeName());

    }

    @Test
    void failedResolve() throws IOException, PlanResolverException {

        // given
        var resolver = DefaultReferencePlanResolver.builder()
                .absoluteResourceLocator(new AbsoluteFileResourceLocator())
                .relativeResourceLocator(RelativeFileResourceLocator.of("src/test/resources/planResolverTest/failedPlan_", "yaml"))
                .build();
        var plan = ModelFactory.readPlanFromYaml(() -> new FileInputStream("src/test/resources/planResolverTest/failedPlan.yaml"));

        // when
        assertThrows(PlanResolverException.class, () -> resolver.resolve(plan));
    }

}