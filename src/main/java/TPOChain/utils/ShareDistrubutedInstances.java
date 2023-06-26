package TPOChain.utils;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

// TPOChainProto和TPOChainData层共享的数据结构
public interface ShareDistrubutedInstances {
    //局部日志
    Map<InetAddress,Map<Integer, InstanceState>> instances = new HashMap<>(8000);
    
    // 局部日志的配置表
    Map<InetAddress,RuntimeConfigure>  hostConfigureMap=new HashMap<>();
    
    // 局部日志现在已经收到多少，是已经存放在局部日志表中
    Map<InetAddress, Integer>   hostReceive=new HashMap<>();
}
