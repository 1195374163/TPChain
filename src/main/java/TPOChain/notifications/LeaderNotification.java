package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;

public class LeaderNotification extends ProtoNotification {
    
    public static final short NOTIFICATION_ID = 202;
    
    private final Host leader;
    
    
    
    public LeaderNotification(Host leader) {
        super(NOTIFICATION_ID);
        this.leader=leader;
    }



    public Host getLeader() {
        return leader;
    }
}
