package TPOChain.messages;

import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.io.IOException;

public class QueryOldChannelUserMsg extends ProtoMessage {
    public static final short MSG_CODE = 701;
    
    public final int iN;
    public final SeqN sN;
    
    public QueryOldChannelUserMsg(int iN, SeqN sN) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
    }

    @Override
    public String toString() {
        return "QueryOldChannelUserMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<QueryOldChannelUserMsg>() {
        public void serialize(QueryOldChannelUserMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
        }

        public QueryOldChannelUserMsg deserialize(ByteBuf in) throws IOException {
            int iN = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            return new QueryOldChannelUserMsg(iN, sN);
        }
    };
}




