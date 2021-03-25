package sparkengine.plan.model.component.visitor;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.impl.*;

public interface ComponentVisitor {

    String WRAPPER = "wrapper";

    default void visitEmptyComponent(Location location,
                                     EmptyComponent component) throws Exception {
    }

    default void visitInlineComponent(Location location,
                                      InlineComponent component) throws Exception {
    }

    default void visitBatchComponent(Location location,
                                     BatchComponent component) throws Exception {
    }

    default void visitStreamComponent(Location location,
                                      StreamComponent component) throws Exception {
    }

    default void visitSqlComponent(Location location,
                                   SqlComponent component) throws Exception {
    }

    default void visitSchemaValidationComponent(Location location,
                                                SchemaValidationComponent component) throws Exception {
    }

    default void visitEncodeComponent(Location location,
                                      EncodeComponent component) throws Exception {
    }

    default void visitTransformComponent(Location location,
                                         TransformComponent component) throws Exception {
    }

    default void visitReferenceComponent(Location location,
                                         ReferenceComponent component) throws Exception {
    }

    default void visitUnionComponent(Location location,
                                     UnionComponent component) throws Exception {
    }

    default void visitFragmentComponent(Location location,
                                        FragmentComponent component) throws Exception {
        ComponentsVisitor.visitComponents(location, this, component.getComponents());
    }

    default void visitWrapperComponent(Location location,
                                       WrapperComponent component) throws Exception {
        ComponentsVisitor.visitComponent(location.push(WRAPPER), this, component.getComponent());
    }

    default void visitMapComponent(Location location,
                                   MapComponent component) throws Exception {
    }

}
