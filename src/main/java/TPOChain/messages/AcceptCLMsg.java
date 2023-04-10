package TPOChain.messages;

import TPOChain.utils.SeqN;
import common.values.PaxosValue;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class AcceptCLMsg extends ProtoMessage {

    public static final short MSG_CODE = 302;

    public final int iN;
    public final Host node;

    public final SeqN sN;

    public final short nodeCounter;
    public final int ack;

    public AcceptCLMsg(int iN,Host node,SeqN sN, short nodeCounter, int ack) {
        super(MSG_CODE);
        this.iN = iN;
        this.node=node;
        this.sN = sN;
        this.nodeCounter = nodeCounter;
        this.ack = ack;
    }

    @Override
    public String toString() {
        return "AcceptMsg{" +
                "iN=" + iN +
                "node=" + node +
                ", sN=" + sN +
                ", nodeCounter=" + nodeCounter +
                ", ack=" + ack +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptCLMsg>() {
        public void serialize(AcceptCLMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            Host.serializer.serialize(msg.node,out);
            msg.sN.serialize(out);
            out.writeShort(msg.nodeCounter);
            out.writeInt(msg.ack);
        }

        /**
         * public final Host node;
         * this.node=node;
         *  "node=" + node +
         * Host.serializer.serialize(msg.node,out);
         * Host t=Host.serializer.deserialize(in);
         * */
        public AcceptCLMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            Host t=Host.serializer.deserialize(in);
            SeqN sN = SeqN.deserialize(in);
            short nodeCount = in.readShort();
            int ack = in.readInt();
            return new AcceptCLMsg(instanceNumber, t,sN, nodeCount,ack);
        }
    };
}
