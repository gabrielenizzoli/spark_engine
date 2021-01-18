package sparkengine.plan.model.component;

import sparkengine.plan.model.encoder.DataEncoder;

public interface EncodedComponent extends Component {

    DataEncoder getEncodedAs();

}
