package TPOChain.utils;

//主要用来对节点执行前链节点的分发命令情况做标记，由节点自动管理
//每个
public class RuntimeConfigure {
    public RuntimeConfigure(){}
    //各个command leader进行命令的分发
    //某个节点在故障恢复后，又成为新的前链节点，那前链节点使用这个
    //其他节点不使用用这个
    public int lastAcceptSent = -1;
   
   
    //每个节点在收到前链节点时对这三条信息进行更改
    public int highestAcceptedInstance = -1;
    
    public int highestDecidedInstance = -1;
    public int highestAcknowledgedInstance = -1;
    
    
    //TODO：这个参数不需要吧:因为这个类主要在
    /**
     * 标记每个节点的accptCl
     * */
    public int   acceptedupcl=-1;
}
