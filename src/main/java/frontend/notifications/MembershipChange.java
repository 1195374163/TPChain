package frontend.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.net.InetAddress;
import java.util.List;

public class MembershipChange extends ProtoNotification {

    public final static short NOTIFICATION_ID = 102;

    private final List<InetAddress> orderedMembers;
    private final InetAddress writesTo;
    private final InetAddress readsTo;
    private final InetAddress writeResponder;
    private  int  index=-1;

    
    public MembershipChange(List<InetAddress> orderedMembers, InetAddress readsTo,
                            InetAddress writesTo, InetAddress writeResponder) {
        super(NOTIFICATION_ID);
        this.orderedMembers = orderedMembers;
        this.readsTo = readsTo;
        this.writesTo = writesTo;
        this.writeResponder = writeResponder;
       
    }
    
    public MembershipChange(List<InetAddress> orderedMembers, InetAddress readsTo,
                            InetAddress writesTo, InetAddress writeResponder,int index) {
        super(NOTIFICATION_ID);
        this.orderedMembers = orderedMembers;
        this.readsTo = readsTo;
        this.writesTo = writesTo;
        this.writeResponder = writeResponder;
        this.index=index;
    }

    public int getIndex() {
        return index;
    }

    public InetAddress getReadsTo() {
        return readsTo;
    }

    public InetAddress getWritesTo() {
        return writesTo;
    }

    public InetAddress getWriteResponder() {
        return writeResponder;
    }

    public List<InetAddress> getOrderedMembers() {
        return orderedMembers;
    }
}
