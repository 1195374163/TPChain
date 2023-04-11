package TPOChain.utils;

//主要用来对节点执行情况做标记，由节点自动管理
public class RuntimeConfigure {

    //各个command leader进行命令的分发
    public int highestAcceptedInstance = -1;
    public int highestAcknowledgedInstance = -1;

    public int highestDecidedInstance = -1;

    public int lastAcceptSent = -1;


    /**
     * 标记每个节点的accptCl
     * */
    public int   acceptedupcl=-1;

}
