package dataengine.pipeline.model.description.source;

import java.util.List;

public interface TransformationComponentWithMultipleInputs extends TransformationComponent {

    List<String> getUsing();

}
