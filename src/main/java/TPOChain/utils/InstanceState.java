package TPOChain.utils;

import TPOChain.ipc.SubmitReadRequest;
import common.values.PaxosValue;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;
/**
 * 局部日志条目：instance的实例 
 * commandleader除了发送appbatch分发消息，还有NOOP消息对之前消息的ack信息，
 * */
public class InstanceState  {
    //要对SeqN进行改造，Seq的Node节点标记从哪个commandleader发来的
    public SeqN highestAccept;
    public final int iN;
    public PaxosValue acceptedValue;
    public short counter;
    private boolean decided;
    
    //不需要,因为在SN中包含了当前实例使用的线程
    //public  short  threadid;
    
    public InstanceState(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.acceptedValue = null;
        this.counter = 0;
        this.decided = false;
    }

    @Override
    public String toString() {
        return "InstanceState{" +
                "iN=" + iN +
                ", highestAccept=" + highestAccept +
                ", acceptedValue=" + acceptedValue +
                ", counter=" + counter +
                ", decided=" + decided +
                '}';
    }
    


    //TODO    forceAccept和accpt区别
    //If it is already decided by some node, or received from prepareOk
    /**
     * 更新SeqN和counter信息，准备重新发送
     * */
    public void forceAccept(SeqN sN, PaxosValue value) {
        assert sN.getCounter() > -1;
        assert value != null;
        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
        assert !isDecided() || acceptedValue.equals(value);
        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue.equals(value);

        this.highestAccept = sN.greaterThan(this.highestAccept) ? sN : this.highestAccept;
        this.acceptedValue = value;
        this.counter = -1;
    }

    //在对对应的instance重新进行赋值
    public void accept(SeqN sN, PaxosValue value, short counter) {
        assert sN.getCounter() > -1;
        assert value != null;
        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
        assert !isDecided() || acceptedValue.equals(value);
        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue.equals(value);

        this.highestAccept = sN;
        this.acceptedValue = value;
        this.counter = counter;
    }

    public boolean isDecided() {
        return decided;
    }

    public void markDecided() {
        assert acceptedValue != null && highestAccept != null;
        assert !decided;
        decided = true;
    }
}
