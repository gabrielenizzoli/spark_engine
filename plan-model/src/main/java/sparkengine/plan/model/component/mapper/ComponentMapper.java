package sparkengine.plan.model.component.mapper;

import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.*;

import java.util.Stack;

public interface ComponentMapper {

    default Component mapEmptyComponent(Stack<String> location,
                                        EmptyComponent component) throws Exception {
        return component;
    }

    default Component mapInlineComponent(Stack<String> location,
                                         InlineComponent component) throws Exception {
        return component;
    }

    default Component mapBatchComponent(Stack<String> location,
                                        BatchComponent component) throws Exception {
        return component;
    }

    default Component mapStreamComponent(Stack<String> location,
                                         StreamComponent component) throws Exception {
        return component;
    }

    default Component mapSqlComponent(Stack<String> location,
                                      SqlComponent component) throws Exception {
        return component;
    }

    default Component mapSchemaValidationComponent(Stack<String> location,
                                                   SchemaValidationComponent component) throws Exception {
        return component;
    }

    default Component mapEncodeComponent(Stack<String> location,
                                         EncodeComponent component) throws Exception {
        return component;
    }

    default Component mapTransformComponent(Stack<String> location,
                                            TransformComponent component) throws Exception {
        return component;
    }

    default Component mapReferenceComponent(Stack<String> location,
                                            ReferenceComponent component) throws Exception {
        return component;
    }

    default Component mapUnionComponent(Stack<String> location,
                                        UnionComponent component) throws Exception {
        return component;
    }

    default Component mapFragmentComponent(Stack<String> location,
                                           FragmentComponent component) throws Exception {
        return component;
    }

    default Component mapWrapperComponent(Stack<String> location,
                                          WrapperComponent component) throws Exception {
        return component;
    }

}
