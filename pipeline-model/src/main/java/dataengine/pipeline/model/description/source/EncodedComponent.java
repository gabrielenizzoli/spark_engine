package dataengine.pipeline.model.description.source;

import dataengine.pipeline.model.description.encoder.DataEncoder;

public interface EncodedComponent extends Component {

    DataEncoder getEncodedAs();

}
