package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;

public class FrontChainNotification  extends ProtoNotification {
    public static final short NOTIFICATION_ID = 204;
    
    List<Host>  frontChain;
    
    public FrontChainNotification(List<Host> mem) {
        super(NOTIFICATION_ID);
        frontChain=mem;
    }

    public List<Host> getFrontChain() {
        return frontChain;
    }
}
