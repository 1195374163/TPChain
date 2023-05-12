package TPOChain.utils;

//主要用来对节点执行前链节点的分发命令情况做标记，由节点自动管理
//TODO  新加入的节点要保存一份这个，还要生成一份局部日志
public class RuntimeConfigure {
    public RuntimeConfigure(){}
    
    //记录各个command leader进行命令的分发的序号
    //某个节点在故障恢复后，又成为新的前链节点，那前链节点使用这个
    //其他节点不使用用这个
    public int lastAcceptSent = -1;
   
   
    //每个节点在收到前链节点时对这三条信息进行更改
    public int highestAcceptedInstance = -1;
    
    public int highestDecidedInstance = -1;
    public int highestAcknowledgedInstance = -1;
    
    
    
    // todo 主要后链链尾用来定时向命令的coommandleader发送ack信息
    // 节点在收到这个节点的分发消息的时间
    public long lastAcceptTime=0;
}