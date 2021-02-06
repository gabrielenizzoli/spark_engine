package sparkengine.plan.model;

public interface Reference {

    ReferenceMode getMode();

    String getRef();

    enum ReferenceMode {
        RELATIVE,
        ABSOLUTE
    }
}
