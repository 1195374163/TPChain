package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class LeaderNotification extends ProtoNotification {
    
    public static final short NOTIFICATION_ID = 202;
    
    private final Host leader;
    
    private  final  boolean canHandleRequest;
    
    
    public LeaderNotification(Host leader,boolean canHandleRequest) {
        super(NOTIFICATION_ID);
        this.leader=leader;
        this.canHandleRequest=canHandleRequest;
    }

    public boolean isCanHandleRequest() {
        return canHandleRequest;
    }

    public Host getLeader() {
        return leader;
    }
}
