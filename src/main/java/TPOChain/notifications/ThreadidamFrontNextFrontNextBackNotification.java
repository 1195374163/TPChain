package TPOChain.notifications;


import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;

public class ThreadidamFrontNextFrontNextBackNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 201;
    
    private final boolean  amFront;
    private final short threadid;
    private final InetAddress nextOkFront;
    private final InetAddress nextOkBack;
    
    public ThreadidamFrontNextFrontNextBackNotification(boolean  amFront,short threadid,InetAddress nextOkFront,InetAddress nextOkBack) {
        super(NOTIFICATION_ID);
        this.amFront=amFront;
        this.threadid=threadid;
        this.nextOkFront=nextOkFront;
        this.nextOkBack=nextOkBack;
    }

    @Override
    public String toString() {
        return "ThreadidamFrontNextFrontNextBackNotification{" +
                "amFront=" + amFront +
                ", threadid=" + threadid +
                ", nextOkFront=" + nextOkFront +
                ", nextOkBack=" + nextOkBack +
                '}';
    }
    public boolean isAmFront() {
        return amFront;
    }
    
    public short getThreadid() {
        return threadid;
    }
    
    public InetAddress getNextOkFront() {
        return nextOkFront;
    }
    
    public InetAddress getNextOkBack() {
        return nextOkBack;
    }
}
