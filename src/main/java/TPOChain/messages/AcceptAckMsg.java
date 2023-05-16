package TPOChain.messages;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class AcceptAckMsg extends ProtoMessage {

    public static final short MSG_CODE = 201;
    
    public final int instanceNumber;

    public AcceptAckMsg(int instanceNumber) {
        super(MSG_CODE);
        this.instanceNumber = instanceNumber;
    }

    @Override
    public String toString() {
        return "AcceptAckMsg{" +
                "instanceNumber=" + instanceNumber +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptAckMsg>() {
        public void serialize(AcceptAckMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instanceNumber);
        }

        public AcceptAckMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            return new AcceptAckMsg(instanceNumber);
        }
    };
}