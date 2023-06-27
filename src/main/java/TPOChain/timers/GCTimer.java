package TPOChain.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;


public class GCTimer extends ProtoTimer {

    public static final short TIMER_ID = 802;

    public static final GCTimer instance = new GCTimer();

    private GCTimer() {
        super(TIMER_ID);
    }
    
    @Override
    public ProtoTimer clone() {
        return this;
    }
}
