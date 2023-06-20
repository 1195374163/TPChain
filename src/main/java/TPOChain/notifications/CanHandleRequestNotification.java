package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class CanHandleRequestNotification  extends ProtoNotification {
    public static final short NOTIFICATION_ID = 203;
    
    private final boolean  canHandleRequest;
    
    
    public CanHandleRequestNotification(boolean canHandleRequest) {
        super(NOTIFICATION_ID);
        this.canHandleRequest=canHandleRequest;
    }

    
    public boolean isCanHandleRequest() {
        return canHandleRequest;
    }
}
