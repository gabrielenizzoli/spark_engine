package sparkengine.plan.model.common;

public interface Reference {

    ReferenceMode getMode();

    String getRef();

    enum ReferenceMode {
        RELATIVE,
        ABSOLUTE
    }
}
