package TPOChain.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ForgetStateTimer extends ProtoTimer {

    public static final short TIMER_ID = 207;

    public static final ForgetStateTimer instance = new ForgetStateTimer();

    private ForgetStateTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
