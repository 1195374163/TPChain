package TPOChain.utils;

import TPOChain.ipc.SubmitReadRequest;
import common.values.PaxosValue;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

// 全局日志条目：
// leader除了发送SortPaxos排序消息，还有成员的更改消息即MembershipOP，还有NOOP消息，

public class InstanceStateCL {

    public final int iN;
    public SeqN highestAccept;
    public PaxosValue acceptedValue;
    
    //ack的数量
    public short counter;
    private boolean decided;
    
    public Map<SeqN, Set<Host>> prepareResponses;
    
    //附加的读,用于读取操作
    private Map<Short, Queue<Long>> attachedReads;


    
    //日志，节点根据消息在本地生成对应的instanceStateCL
    //哪几种情况会用到这个
    //1 附加读的情况下
    //2 进行prepare选举时用到这个 
    //3 正常接收到新的序号的消息
    public InstanceStateCL(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.acceptedValue=null;
        this.counter = 0;
        this.decided = false;
        
        this.prepareResponses = new HashMap<>();
        this.attachedReads = new HashMap<>();
    }

    @Override
    public String toString() {
        return "InstanceStateCL{" +
                "iN=" + iN +
                ", highestAccept=" + highestAccept +
                ", acceptedValue=" + acceptedValue +
                ", counter=" + counter +
                ", decided=" + decided +
                ", prepareResponses=" + prepareResponses +
                '}';
    }
    
    //read只挂载未decided的日志条目
    public void attachRead(SubmitReadRequest request) {
        if (decided) throw new IllegalStateException();
        attachedReads.computeIfAbsent(request.getFrontendId(), k -> new LinkedList<>()).add(request.getBatchId());
    }

    public Map<Short, Queue<Long>> getAttachedReads() {
        return attachedReads;
    }

    
    //TODO 这个forceAccept用在什么情况
    //  在接收到其他节点的decideClMsg信息时，用到了这个
    //If it is already decided by some node, or received from prepareOk
    /**
     * 更新SeqN和counter信息，准备重新发送
     * */
    public void forceAccept(SeqN sN, PaxosValue acceptedValue) {
//        assert sN.getCounter() > -1;
//        assert value != null;
//        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
//        assert !isDecided() || acceptedValue.equals(value);
//        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue.equals(value);
        this.highestAccept = sN.greaterThan(this.highestAccept) ? sN : this.highestAccept;
        this.acceptedValue=acceptedValue;
        this.counter = -1;
    }
    
    //对instance进行投票
    public void accept(SeqN sN,PaxosValue acceptedValue, short counter) {
        this.highestAccept = sN;
        this.acceptedValue=acceptedValue;
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
