package TPOChain.utils;


import common.values.PaxosValue;

/**
 * 局部日志条目：instance的实例 
 * commandleader除了发送appbatch分发消息，还有NOOP消息对之前消息的ack信息，
 * */
public class InstanceState  {
    public final int iN;
    //要对SeqN进行改造，Seq的Node节点标记从哪个commandleader发来的
    public SeqN highestAccept;
    public PaxosValue acceptedValue;
    public short counter;
    private boolean decided;
    
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
