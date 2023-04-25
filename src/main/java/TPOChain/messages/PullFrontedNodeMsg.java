package TPOChain.messages;

import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.io.IOException;

//TODO 拉取后链首节点成为前链节点 
// 时机时机在删除节点时

public class PullFrontedNodeMsg extends ProtoMessage {
    
    public static final short MSG_CODE = 305;
    
    public final int iN;
    public final SeqN sN;

    public PullFrontedNodeMsg(int iN, SeqN sN) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
    }

    @Override
    public String toString() {
        return "PrepareMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<PullFrontedNodeMsg>() {
        public void serialize(PullFrontedNodeMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
        }

        public PullFrontedNodeMsg deserialize(ByteBuf in) throws IOException {
            int iN = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            return new PullFrontedNodeMsg(iN, sN);
        }
    };
}