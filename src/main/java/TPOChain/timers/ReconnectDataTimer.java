package TPOChain.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;



public class ReconnectDataTimer extends ProtoTimer {
    public static final short TIMER_ID = 304;
    private final Host host;
    public ReconnectDataTimer(Host host) {
        super(TIMER_ID);
        this.host = host;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

}
