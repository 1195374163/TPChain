package TPOChain.utils;


import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;


// TPOChainProto和TPOChainData层共享的数据结构
public interface ShareDistrubutedInstances {
    
    //局部日志
    Map<InetAddress,Map<Integer, InstanceState>> instances = new HashMap<>(6000);
    
    
    // 局部日志的配置表
    Map<InetAddress,RuntimeConfigure>  hostConfigureMap=new HashMap<>();
}
