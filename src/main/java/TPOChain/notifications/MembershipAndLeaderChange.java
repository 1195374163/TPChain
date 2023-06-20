package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;
import java.util.List;

public class MembershipAndLeaderChange extends ProtoNotification {
    
    public final static short NOTIFICATION_ID = 202;
    
    private final List<InetAddress> orderedMembers;
    
    private final Host leader;
    
    public MembershipAndLeaderChange(List<InetAddress> orderedMembers, 
                            Host leader) {
        super(NOTIFICATION_ID);
        this.orderedMembers = orderedMembers;
        this.leader = leader;
    }



    public Host getLeader() {
        return leader;
    }


    public List<InetAddress> getOrderedMembers() {
        return orderedMembers;
    }
}