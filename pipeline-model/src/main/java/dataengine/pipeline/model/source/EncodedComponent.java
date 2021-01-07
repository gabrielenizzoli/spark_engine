package dataengine.pipeline.model.source;

import dataengine.pipeline.model.encoder.DataEncoder;

public interface EncodedComponent extends Component {

    DataEncoder getEncodedAs();

}
