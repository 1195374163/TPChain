package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class FrontIndexNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 206;
    
    public  int index;
    public FrontIndexNotification(int index) {
        super(NOTIFICATION_ID);
        this.index=index;
    }

    public int getIndex() {
        return index;
    }
}
