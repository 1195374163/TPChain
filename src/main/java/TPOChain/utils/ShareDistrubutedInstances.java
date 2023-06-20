package TPOChain.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashMap;
import java.util.Map;


// TPOChainProto和TPOChainData层共享的数据结构
public interface ShareDistrubutedInstances {
    
    //局部日志
    Map<Host,Map<Integer, InstanceState>> instances = new HashMap<>(6000);
}
