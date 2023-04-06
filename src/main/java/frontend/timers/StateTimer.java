package frontend.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

//ProtoTimer： Abstract Timer class to be extended by protocol-specific timers.

public class StateTimer extends ProtoTimer {

    public static final short TIMER_ID = 102;

    public StateTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
