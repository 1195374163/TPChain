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

    
    public AcceptAckCLMsg(int instanceNumber) {
        super(MSG_CODE);
        this.instanceNumber = instanceNumber;
    }

    @Override
    public String toString() {
        return "AcceptAckCLMsg{" +
                "instanceNumber=" + instanceNumber +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptAckCLMsg>() {
        public void serialize(AcceptAckCLMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instanceNumber);
        }

        public AcceptAckCLMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            return new AcceptAckCLMsg(instanceNumber);
        }
    };
}