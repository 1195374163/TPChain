package TPOChain.utils;


import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import java.io.IOException;

/**
 * 全局排序的accpet信息
 * */
public class AcceptedValueCL {
    public final int instance;
    public final SeqN sN;
//    public final PaxosValue value;
    public Host node;//哪个commandleader发出的
    public int  sequence;//在commandleader的第几个位置

    public AcceptedValueCL(int instance, SeqN sN, Host node,int sequence) {
        this.instance = instance;
        this.sN = sN;
//      this.value = value;
        this.node=node;
        this.sequence=sequence;
    }

    @Override
    public String toString() {
        return "AV{" +
                "i=" + instance +
                ", sn=" + sN +
                ", host=" + node +
                ", sequence=" + sequence +
                '}';
    }

    public void serialize(ByteBuf out) throws IOException {
        out.writeInt(instance);
        sN.serialize(out);
        //PaxosValue.serializer.serialize(value, out);
        Host.serializer.serialize(node,out);
        out.writeInt(sequence);
    }

    public static AcceptedValueCL deserialize(ByteBuf in) throws IOException {
        int acceptedInstance = in.readInt();
        SeqN sN = SeqN.deserialize(in);
        //PaxosValue acceptedValue = PaxosValue.serializer.deserialize(in);
        Host temp=Host.serializer.deserialize(in);
        int  t=in.readInt();
        return new AcceptedValueCL(acceptedInstance, sN, temp,t);
    }
}
