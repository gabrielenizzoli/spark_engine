package sparkengine.plan.model.component;

import java.util.List;

public interface ComponentWithMultipleInputs extends Component {

    List<String> getUsing();

}
