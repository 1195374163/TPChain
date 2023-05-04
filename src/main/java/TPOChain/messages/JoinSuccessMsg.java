package TPOChain.messages;

import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinSuccessMsg extends ProtoMessage {

    public static final short MSG_CODE = 205;

    public final int iN;
    public final SeqN sN;
    //public final List<Host> membership;
    public final Pair<List<Host>, Map<Host,Boolean>> membership;
    
    public JoinSuccessMsg(int iN, SeqN sN, Pair<List<Host>, Map<Host,Boolean>> membership) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.membership = membership;
    }

    @Override
    public String toString() {
        return "JoinSuccessMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", membership=" + membership +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<JoinSuccessMsg>() {
        public void serialize(JoinSuccessMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            
            out.writeInt(msg.membership.getLeft().size());
            for (Host h : msg.membership.getLeft())
                Host.serializer.serialize(h, out);
            
            //TODO 对一个Map类型进行序列化
            out.writeInt(msg.membership.getRight().size());
            for (Map.Entry<Host, Boolean> entry : msg.membership.getRight().entrySet()) {
                Host.serializer.serialize(entry.getKey(), out);
                out.writeBoolean(entry.getValue());
            }
        }

        public JoinSuccessMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN seqN = SeqN.deserialize(in);
            
            int membershipSize = in.readInt();
            List<Host> membership = new ArrayList<>(membershipSize);
            for (int i = 0; i < membershipSize; i++)
                membership.add(Host.serializer.deserialize(in));
            
            int membershipMapSize = in.readInt();
            Map<Host, Boolean> membershipMap = new HashMap<>(membershipMapSize);
            for (int i = 0; i < membershipMapSize; i++) {
                Host host = Host.serializer.deserialize(in);
                boolean value = in.readBoolean();
                membershipMap.put(host, value);
            }
            return new JoinSuccessMsg(instanceNumber, seqN, new MutablePair<>(membership,membershipMap));
        }
    };
}
