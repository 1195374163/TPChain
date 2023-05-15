package TPOChain.messages;

import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;



public class ElectionSuccessMsg extends ProtoMessage {

    public static final short MSG_CODE = 405;

    public final int iN;
    public final SeqN sN;


    public ElectionSuccessMsg(int iN, SeqN sN) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
    }

    @Override
    public String toString() {
        return "ElectionSuccessMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<ElectionSuccessMsg>() {
        public void serialize(ElectionSuccessMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
        }

        public ElectionSuccessMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN seqN = SeqN.deserialize(in);
            return new ElectionSuccessMsg(instanceNumber, seqN);
        }
    };
}
