package TPOChain.notifications;

import frontend.ops.OpBatch;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class ThreadidamFrontNextFrontNextBackNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 201;
    
    private final boolean  amFront;
    private final short threadid;
    private final Host nextOkFront;
    private final Host nextOkBack;
    
    public ThreadidamFrontNextFrontNextBackNotification(boolean  amFront,short threadid,Host nextOkFront,Host nextOkBack) {
        super(NOTIFICATION_ID);
        this.amFront=amFront;
        this.threadid=threadid;
        this.nextOkFront=nextOkFront;
        this.nextOkBack=nextOkBack;
    }
    
    public boolean isAmFront() {
        return amFront;
    }
    
    public short getThreadid() {
        return threadid;
    }
    
    public Host getNextOkFront() {
        return nextOkFront;
    }
    
    public Host getNextOkBack() {
        return nextOkBack;
    }
}
