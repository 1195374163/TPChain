package TPOChain.notifications;

import TPOChain.TPOChainProto;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

//只要通知新加入节点的Data在uponoutconnectionup()不要向Leader发送消息，因为它的日志是空的，直接return；
public class StateNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 309;
    
    private final TPOChainProto.State state;
    
    public StateNotification(TPOChainProto.State state) {
        super(NOTIFICATION_ID);
        this.state = state;
    }

    public TPOChainProto.State getState() {
        return state;
    }
}
