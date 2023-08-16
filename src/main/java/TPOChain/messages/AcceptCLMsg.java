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

    public final PaxosValue value;
    
    public final SeqN sN;

    public final short nodeCounter;
    public final int ack;
    
    
    
    public AcceptCLMsg(int iN,SeqN sN, short nodeCounter,PaxosValue value,int ack) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.value=value;
        this.nodeCounter = nodeCounter;
        this.ack = ack;
    }

    @Override
    public String toString() {
        return "AcceptCLMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", nodeCounter=" + nodeCounter +
                ", ack=" + ack +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptCLMsg>() {
        public void serialize(AcceptCLMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            out.writeShort(msg.nodeCounter);
            out.writeInt(msg.ack);
            
            
            PaxosValue.serializer.serialize(msg.value, out);
        }
        
        public AcceptCLMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            short nodeCount = in.readShort();
            int ack = in.readInt();
            
            
            
            PaxosValue payload = PaxosValue.serializer.deserialize(in);
            return new AcceptCLMsg(instanceNumber, sN, nodeCount,payload,ack);
        }
    };
}
