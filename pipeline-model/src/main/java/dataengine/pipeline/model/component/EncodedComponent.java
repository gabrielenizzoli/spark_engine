package dataengine.pipeline.model.component;

import dataengine.pipeline.model.encoder.DataEncoder;

public interface EncodedComponent extends Component {

    DataEncoder getEncodedAs();

}
