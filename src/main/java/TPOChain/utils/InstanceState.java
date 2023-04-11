package TPOChain.utils;

import TPOChain.ipc.SubmitReadRequest;
import common.values.PaxosValue;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;
/**
 * instance的实例
 * */
public class InstanceState {

    public final int iN;
    //要对SeqN进行改造，Seq的Node节点标记从哪个commandleader发来的
    public SeqN highestAccept;
    public PaxosValue acceptedValue;
    public short counter;
    private boolean decided;
    //这个要去掉，
    public Map<SeqN, Set<Host>> prepareResponses;

    //附加的读  这个要去掉
    private Map<Short, Queue<Long>> attachedReads;
    
    

    /**
     * c-instance和o-instance是否都发了的标记位，在新leader选举时，考虑到是否向leader发送request问题。
     */
    //private  boolean   coconcurrency;
    public InstanceState(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.acceptedValue = null;
        this.counter = 0;
        this.decided = false;
        this.prepareResponses = new HashMap<>();
        this.attachedReads = new HashMap<>();
    }

    @Override
    public String toString() {
        return "InstanceState{" +
                "iN=" + iN +
                ", highestAccept=" + highestAccept +
                ", acceptedValue=" + acceptedValue +
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
