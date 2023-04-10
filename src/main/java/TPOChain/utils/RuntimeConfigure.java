package TPOChain.utils;

//主要用来对节点执行情况做标记，由节点自动管理
public class RuntimeConfigure {
    //各个command leader进行命令的分发
    public int highestAcknowledgedInstance = -1;

    public int highestAcceptedInstance = -1;

    public int highestDecidedInstance = -1;

    public int lastAcceptSent = -1;


    //leader的排序字段
    public int highestAcknowledgedInstanceCL = -1;
    public int highestAcceptedInstanceCL = -1;
    public int highestDecidedInstanceCl = -1;
    public int lastAcceptSentCL = -1;

}
