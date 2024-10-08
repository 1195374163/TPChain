package TPOChain.messages;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class AcceptAckMsg extends ProtoMessage {

    public static final short MSG_CODE = 501;
    public  final  Host node;
    public final int instanceNumber;
    
    
    public AcceptAckMsg(Host node,int instanceNumber) {
        super(MSG_CODE);
        this.node=node;
        //this.threadid=threadid;//这里继承了父类的变量
        this.instanceNumber = instanceNumber;
    }

    @Override
    public String toString() {
        return "AcceptAckMsg{" +
                "node="+ node +
                //"threadid="+ threadid +
                "instanceNumber=" + instanceNumber +
                '}';
    }

    
    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptAckMsg>() {
        public void serialize(AcceptAckMsg msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.node,out);
            //out.writeShort(msg.threadid);
            out.writeInt(msg.instanceNumber);
        }

        public AcceptAckMsg deserialize(ByteBuf in) throws IOException {
            Host node=Host.serializer.deserialize(in);
            //short threadid=in.readShort();
            int instanceNumber = in.readInt();
           //return new AcceptAckMsg(node,threadid,instanceNumber);
            return new AcceptAckMsg(node,instanceNumber);
        }
    };
}