package TPOChain.utils;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;


// TPOChainProto和TPOChainData层共享的数据结构
public interface ShareDistrubutedInstances {
    //局部日志
    Map<InetAddress,Map<Integer, InstanceState>> instances = new HashMap<>(100);
    
    //每一个Data通道都有着自己的消息队列
    Map<InetAddress, BlockingQueue<InstanceState>>  hostMessageQueue=new HashMap<>(100);
    
    
    // 局部日志的配置表
    Map<InetAddress,RuntimeConfigure>  hostConfigureMap=new HashMap<>(100);
}
