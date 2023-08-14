package TPOChain.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//主要用来对节点执行前链节点的分发命令情况做标记，由节点自动管理
//TODO  新加入的节点不仅要复制原有的配置表  消息队列  局部日志表 ，如果原配置中没有还要新生成一个一份新加入节点的
public class RuntimeConfigure {
    // TODO: 2023/7/27 是否给每个配置文件加个锁：加入一个访问更改锁？多个锁呢：访问不同的字段
    public  RuntimeConfigure(){
        
    }
    
    public RuntimeConfigure(int lastAcceptSent, int highestAcceptedInstance,
                            int highestDecidedInstance,int highestAcknowledgedInstance){
        
        this.lastAcceptSent=lastAcceptSent;
        this.highestAcceptedInstance=highestAcceptedInstance;
        this.highestDecidedInstance=highestDecidedInstance;
        this.highestAcknowledgedInstance=highestAcknowledgedInstance;
    }

    // TODO 如果这个长期没有，需要重新向Leader发申请排序
    //  或者Leader 在排序时，发现现在排序 -   上次排序大于1 ，应该对之前的也顺便排序（必须）。
    //  这个可能是bug，先不考虑，后期结果不对，再考虑可能是这个
    public  int   lastOrderInstance=-1;  // leader排序时使用，测试是否是否有空缺

    
    
    //记录各个command leader进行命令的分发的序号，某个节点在故障恢复后，又成为新的前链节点，那前链节点使用这个，其他节点不使用用这个
    public int lastAcceptSent = -1;


    // TODO: 2023/8/3 上次给这个节点排序的号,  Leader使用，确定排序是不是依次的，不是，那么查看局部日志有没有对应缺少排序的消息，对缺少排序可以补充
    public int  lastOrder=-1;
    
    
    //每个节点在收到前链节点时对这三条信息进行更改
    public int highestAcceptedInstance = -1;
    
    public int highestDecidedInstance = -1;


    // TODO: 2023/8/13 将ack和execute 设置两个访问锁 
    // 这个有Data控制
    public int highestAcknowledgedInstance = -1;
    
    //这个由Control进行赋值的
    public int highestExecutedInstance=-1;
    // 由控制协议的执行线程赋值，由各节点的Data 的GC线程使用，错过也不要紧，后续有新值同样顶替旧值的作用
    public BlockingQueue<Integer>  executeFlagQueue= new LinkedBlockingQueue<>();


    //这个是Data通道的GC线程改变和上面的可能同时改变， GC标记不一定非要紧贴着 ack或execute，可以相差100个数
    public  int highestGCInstance=-1;
    // 旧的ack值的队列：在uponacceptMsg()中赋值，在gc线程中使用
    public BlockingQueue<Integer>  ackFlagQueue= new LinkedBlockingQueue<>();
    
    
    
    
    /**
     * 链尾节点使用:主要后链链尾用来定时向消息的发送方或系统全局发送ack信息
     * */


    // 上一次发送accept消息的时间
    public long  lastSendAcceptTime=0;
    //  节点在收到这个节点的分发消息的时间
    public long lastAcceptTime=0;    
    //TODO: 2023/7/26 当取对应的前链节点一直不发送对应消息的Ack消息：可能由于前链节点故障，那么有末尾节点向全局发送ack消息
    public long  lastReceiveAckTime=0;
    
    // 还有个当前时间System.currentTimeMillis()  这个与lastReceiveAckTime之差
    

}