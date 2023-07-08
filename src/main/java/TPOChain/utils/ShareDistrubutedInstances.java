package TPOChain.utils;

import epaxos.utils.Instance;
import pt.unl.fct.di.novasys.babel.internal.InternalEvent;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

// TPOChainProto和TPOChainData层共享的数据结构
public interface ShareDistrubutedInstances {
    //局部日志
    Map<InetAddress,Map<Integer, InstanceState>> instances = new HashMap<>(30000);
    
    // 局部日志的配置表
    Map<InetAddress,RuntimeConfigure>  hostConfigureMap=new HashMap<>();
    
    //每一个Data通道都有着自己的消息队列
    Map<InetAddress, BlockingQueue<InstanceState>>  hostMessageQueue=new HashMap<>();
}
