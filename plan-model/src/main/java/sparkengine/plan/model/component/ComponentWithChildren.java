package sparkengine.plan.model.component;

import java.util.Map;

public interface ComponentWithChildren extends Component {

    Map<String, Component> getComponents();

}
