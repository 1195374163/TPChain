package TPOChain.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//主要用来对节点执行前链节点的分发命令情况做标记，由节点自动管理
//TODO  新加入的节点要保存一份这个，还要生成一份局部日志
public class RuntimeConfigure {
    public  RuntimeConfigure(){}
    
    public RuntimeConfigure(int lastAcceptSent, int highestAcceptedInstance,
                            int highestDecidedInstance,int highestAcknowledgedInstance, 
                            long lastAcceptTime){
        
        this.lastAcceptSent=lastAcceptSent;
        this.highestAcceptedInstance=highestAcceptedInstance;
        this.highestDecidedInstance=highestDecidedInstance;
        this.highestAcknowledgedInstance=highestAcknowledgedInstance;
        this.lastAcceptTime=lastAcceptTime;
    }
    
    //记录各个command leader进行命令的分发的序号
    //某个节点在故障恢复后，又成为新的前链节点，那前链节点使用这个
    //其他节点不使用用这个
    public int lastAcceptSent = -1;
   
   
    //每个节点在收到前链节点时对这三条信息进行更改
    public int highestAcceptedInstance = -1;
    
    public int highestDecidedInstance = -1;
    public int highestAcknowledgedInstance = -1;
    public int highestExecutedInstance=-1;
    // GC标记不一定非要紧贴着 ack或execute，可以相差100个数
    public  int highestGCInstance=-1;
    
    
    // 由控制协议的执行线程，赋值，由各节点的Data 的GC线程使用，错过
    public BlockingQueue<Integer>  executeFlagQueue= new LinkedBlockingQueue<>();
    
    
    // TODO: 2023/5/23 用来向新加入节点转发各个commandleader节点的分发情况 
    //  主要后链链尾用来定时向命令的coommandleader发送ack信息
    //  节点在收到这个节点的分发消息的时间
    // TODO: 2023/5/29 在commandleader发送noop消息，启动一个时钟，到时停止计时并且对noop消息自动进行ack 
    public long lastAcceptTime=0;
    // TODO: 2023/7/26 当取对应的前链节点一直不发送对应消息的Ack消息：可能由于前链节点故障，那么有末尾节点向全局发送ack消息 
}