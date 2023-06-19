package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.net.InetAddress;
import java.util.List;

public class MembershipAndLeaderChange extends ProtoNotification {
    
    public final static short NOTIFICATION_ID = 202;
    
    private final List<InetAddress> orderedMembers;
    
    private final InetAddress leader;
    
    public MembershipAndLeaderChange(List<InetAddress> orderedMembers, InetAddress readsTo,
                            InetAddress writesTo, InetAddress writeResponder) {
        super(NOTIFICATION_ID);
        this.orderedMembers = orderedMembers;
        this.leader = writesTo;
    }



    public InetAddress getLeader() {
        return leader;
    }


    public List<InetAddress> getOrderedMembers() {
        return orderedMembers;
    }
}