package dataengine.pipeline.model.source;

import java.util.List;

public interface TransformationComponentWithMultipleInputs extends TransformationComponent {

    List<String> getUsing();

}
