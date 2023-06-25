package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;
import java.util.List;

public class FrontChainNotification  extends ProtoNotification {
    public static final short NOTIFICATION_ID = 204;
    List<InetAddress>  frontChain;
    List<InetAddress>  backchain;
    
    public FrontChainNotification(List<InetAddress> mem,List<InetAddress> backchain) {
        super(NOTIFICATION_ID);
        frontChain=mem;
        this.backchain=backchain;
    }

    public List<InetAddress> getFrontChain() {
        return frontChain;
    }

    public List<InetAddress> getBackchain() {
        return backchain;
    }
}
