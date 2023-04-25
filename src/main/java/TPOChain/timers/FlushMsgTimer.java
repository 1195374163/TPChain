package TPOChain.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 *每个节点使用进行刷新消息
 * */
//同时包含了对分发实例的commit消息
public class FlushMsgTimer extends ProtoTimer {

    public static final short TIMER_ID = 206;

    public static final FlushMsgTimer instance = new FlushMsgTimer();

    private FlushMsgTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}