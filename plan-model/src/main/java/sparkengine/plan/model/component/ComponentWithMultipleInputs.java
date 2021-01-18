package sparkengine.plan.model.component;

import java.util.List;

public interface ComponentWithMultipleInputs extends TransformationComponent {

    List<String> getUsing();

}
