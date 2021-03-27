package sparkengine.plan.model.plan.visitor;

public class PlanVisitorException extends Exception {

    public PlanVisitorException(String str) {
        super(str);
    }

    public PlanVisitorException(String str, Throwable t) {
        super(str, t);
    }

}
