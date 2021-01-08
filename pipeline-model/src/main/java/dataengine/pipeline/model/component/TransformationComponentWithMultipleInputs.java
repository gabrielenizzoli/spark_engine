package dataengine.pipeline.model.component;

import java.util.List;

public interface TransformationComponentWithMultipleInputs extends TransformationComponent {

    List<String> getUsing();

}
