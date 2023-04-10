package TPOChain.utils;

import TPOChain.ipc.SubmitReadRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class InstanceStateCL {

    public final int iN;
    public SeqN highestAccept;
    //public PaxosValue acceptedValue;
    //上面这个不需要，反而是需要<node，sequence>标识一个acceptedValue
    public Host node;//哪个commandleader发出的
    public int  sequence;//在commandleader的第几个位置

    //ack的数量
    public short counter;
    private boolean decided;
    public Map<SeqN, Set<Host>> prepareResponses;
    //附加的读
    private Map<Short, Queue<Long>> attachedReads;


    /**
     * c-instance和o-instance是否都发了的标记位，在新leader选举时，考虑到是否向leader发送request问题。
     */

    //private  boolean   coconcurrency;
    //重写
    public InstanceStateCL(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.counter = 0;
        this.decided = false;
        this.prepareResponses = new HashMap<>();
        this.attachedReads = new HashMap<>();
        this.node=null;//哪个commandleader发出的
        this.sequence=0;//在commandleader的第几个位置
    }

    @Override
    public String toString() {
        return "InstanceState{" +
                "iN=" + iN +
                ", highestAccept=" + highestAccept +
                ", Host=" + node +
                ", sequence=" + sequence +
                ", counter=" + counter +
                ", decided=" + decided +
                ", prepareResponses=" + prepareResponses +
                '}';
    }

    public void attachRead(SubmitReadRequest request) {
        if (decided) throw new IllegalStateException();
        attachedReads.computeIfAbsent(request.getFrontendId(), k -> new LinkedList<>()).add(request.getBatchId());
    }

    public Map<Short, Queue<Long>> getAttachedReads() {
        return attachedReads;
    }


    //If it is already decided by some node, or received from prepareOk
    /**
     * 更新SeqN和counter信息，准备重新发送
     * */
    public void forceAccept(SeqN sN, Host node,int sequence) {
//        assert sN.getCounter() > -1;
//        assert value != null;
//        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
//        assert !isDecided() || acceptedValue.equals(value);
//        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue.equals(value);
        this.highestAccept = sN.greaterThan(this.highestAccept) ? sN : this.highestAccept;
        this.node=node;//哪个commandleader发出的
        this.sequence=sequence;//在commandleader的第几个位置
        this.counter = -1;
    }

    public void accept(SeqN sN,Host node,int sequence, short counter) {
        this.highestAccept = sN;
        this.node=node;//哪个commandleader发出的
        this.sequence=sequence;//在commandleader的第几个位置
        this.counter = counter;
    }

    public boolean isDecided() {
        return decided;
    }

    public void markDecided() {
//        assert acceptedValue != null && highestAccept != null;
//        assert !decided;
        decided = true;
    }
}
