package sparkengine.plan.model.component.mapper;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.*;

public interface ComponentMapper {

    String WRAPPER = "wrapper";

    default Component mapEmptyComponent(Location location,
                                        EmptyComponent component) throws Exception {
        return component;
    }

    default Component mapInlineComponent(Location location,
                                         InlineComponent component) throws Exception {
        return component;
    }

    default Component mapBatchComponent(Location location,
                                        BatchComponent component) throws Exception {
        return component;
    }

    default Component mapStreamComponent(Location location,
                                         StreamComponent component) throws Exception {
        return component;
    }

    default Component mapSqlComponent(Location location,
                                      SqlComponent component) throws Exception {
        return component;
    }

    default Component mapSchemaValidationComponent(Location location,
                                                   SchemaValidationComponent component) throws Exception {
        return component;
    }

    default Component mapEncodeComponent(Location location,
                                         EncodeComponent component) throws Exception {
        return component;
    }

    default Component mapTransformComponent(Location location,
                                            TransformComponent component) throws Exception {
        return component;
    }

    default Component mapReferenceComponent(Location location,
                                            ReferenceComponent component) throws Exception {
        return component;
    }

    default Component mapUnionComponent(Location location,
                                        UnionComponent component) throws Exception {
        return component;
    }

    default Component mapFragmentComponent(Location location,
                                           FragmentComponent component) throws Exception {
        return component.withComponents(ComponentsMapper.mapComponents(location, this, component.getComponents()));
    }

    default Component mapWrapperComponent(Location location,
                                          WrapperComponent component) throws Exception {
        return component.withComponent(ComponentsMapper.mapComponent(location.push(WRAPPER), this, component.getComponent()));
    }

    default Component mapMapComponent(Location location,
                                          MapComponent component) throws Exception {
        return component;
    }

}
