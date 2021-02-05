package sparkengine.plan.model.component;

import sparkengine.plan.model.encoder.DataEncoder;

public interface ComponentWithEncoder extends Component {

    DataEncoder getEncodedAs();

}
