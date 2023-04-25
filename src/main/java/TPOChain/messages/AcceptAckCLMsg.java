package TPOChain.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;


public class AcceptAckCLMsg extends ProtoMessage {

    public static final short MSG_CODE = 301;
    //全局顺序
    public final int instanceNumber;

    //二元组 标识哪个节点的第几个序号
    public final Host node;
    public final int iN;
    
    


    public AcceptAckCLMsg(int instanceNumber,Host node,int iN) {
        super(MSG_CODE);
        this.instanceNumber = instanceNumber;
        this.node=node;
        this.iN=iN;
    }

    @Override
    public String toString() {
        return "AcceptAckCLMsg{" +
                "instanceNumber=" + instanceNumber +
                "node=" + node +
                "iN=" + iN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptAckCLMsg>() {
        public void serialize(AcceptAckCLMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instanceNumber);
            Host.serializer.serialize(msg.node,out);
            out.writeInt(msg.iN);
        }

        public AcceptAckCLMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            Host t=Host.serializer.deserialize(in);
            int iN = in.readInt();
            return new AcceptAckCLMsg(instanceNumber,t,iN);
        }
    };
}