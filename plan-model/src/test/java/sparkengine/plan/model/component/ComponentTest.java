package sparkengine.plan.model.component;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.component.impl.EncodeComponent;
import sparkengine.plan.model.component.impl.FragmentComponent;
import sparkengine.plan.model.component.impl.SqlComponent;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ComponentTest {

    @Test
    void getComponentTypeMap() {
        assertEquals(EncodeComponent.TYPE_NAME, Component.COMPONENT_TYPE_MAP.get(EncodeComponent.class), EncodeComponent.class.getSimpleName());
        assertEquals(SqlComponent.TYPE_NAME, Component.COMPONENT_TYPE_MAP.get(SqlComponent.class), SqlComponent.class.getSimpleName());
        assertEquals(FragmentComponent.TYPE_NAME, Component.COMPONENT_TYPE_MAP.get(FragmentComponent.class), FragmentComponent.class.getSimpleName());
    }

    @Test
    void getComponentNameMap() {
        assertEquals(EncodeComponent.class, Component.COMPONENT_NAME_MAP.get(EncodeComponent.TYPE_NAME), EncodeComponent.class.getSimpleName());
        assertEquals(SqlComponent.class, Component.COMPONENT_NAME_MAP.get(SqlComponent.TYPE_NAME), SqlComponent.class.getSimpleName());
        assertEquals(FragmentComponent.class, Component.COMPONENT_NAME_MAP.get(FragmentComponent.TYPE_NAME), FragmentComponent.class.getSimpleName());
    }

}