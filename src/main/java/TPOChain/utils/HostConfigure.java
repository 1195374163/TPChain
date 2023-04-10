package TPOChain.utils;

import java.lang.module.FindException;

public class HostConfigure {
    public int  index=-1;
    public boolean  isFrontedChainNode=false;
    public boolean isFault=false;

    public HostConfigure(int index,boolean isFrontedChainNode,boolean isFault){
        this.index=index;
        this.isFrontedChainNode=isFrontedChainNode;
        this.isFault=isFault;
    }

}
