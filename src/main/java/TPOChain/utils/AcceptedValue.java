package TPOChain.utils;

import common.values.PaxosValue;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
/**
 * 代表已经被系统决定的实例，需要被其他落后节点  学习  的实例
 * */
public class AcceptedValue {
    public final int instance;
    public final SeqN sN;
    public final PaxosValue value;

    public AcceptedValue(int instance, SeqN sN, PaxosValue value) {
        this.instance = instance;
        this.sN = sN;
        this.value = value;
    }

    @Override
    public String toString() {
        return "AV{" +
                "i=" + instance +
                ", sn=" + sN +
                ", v=" + value +
                '}';
    }

    public void serialize(ByteBuf out) throws IOException {
        out.writeInt(instance);
        sN.serialize(out);
        PaxosValue.serializer.serialize(value, out);
    }

    public static AcceptedValue deserialize(ByteBuf in) throws IOException {
        int acceptedInstance = in.readInt();
        SeqN sN = SeqN.deserialize(in);
        PaxosValue acceptedValue = PaxosValue.serializer.deserialize(in);
        return new AcceptedValue(acceptedInstance, sN, acceptedValue);
    }
}