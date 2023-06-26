package TPOChain.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class InitializeCompletedNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 207;
    public InitializeCompletedNotification() {
        super(NOTIFICATION_ID);
    }
}