package TPOChain.messages;

import TPOChain.utils.AcceptedValue;
import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class QueryOldChannelUserOkMsg extends ProtoMessage{
    public static final short MSG_CODE = 702;

    public final int iN;
    public final SeqN sN;
    public final List<AcceptedValue> acceptedValue;
    
    public QueryOldChannelUserOkMsg(int iN, SeqN sN, List<AcceptedValue> acceptedCLValues) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.acceptedValue = acceptedCLValues;
    }

    @Override
    public String toString() {
        return "PrepareOkMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", acceptedValues=" + acceptedValue +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<QueryOldChannelUserOkMsg>() {
        public void serialize(QueryOldChannelUserOkMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            out.writeInt(msg.acceptedValue.size());
            for (AcceptedValue v : msg.acceptedValue) {
                v.serialize(out);
            }
        }

        public QueryOldChannelUserOkMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            int acceptedValuesLength = in.readInt();
            List<AcceptedValue> acceptedValues = new ArrayList<>(acceptedValuesLength);
            for (int i = 0; i < acceptedValuesLength; i++) {
                acceptedValues.add(AcceptedValue.deserialize(in));
            }
            return new QueryOldChannelUserOkMsg(instanceNumber, sN, acceptedValues);
        }
    };
}