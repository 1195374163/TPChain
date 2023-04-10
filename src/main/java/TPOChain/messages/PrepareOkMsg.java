package TPOChain.messages;

import TPOChain.utils.AcceptedValue;
import TPOChain.utils.AcceptedValueCL;
import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PrepareOkMsg extends ProtoMessage {
    public static final short MSG_CODE = 208;

    public final int iN;
    public final SeqN sN;
    public final List<AcceptedValueCL> acceptedValueCLS;


    public PrepareOkMsg(int iN, SeqN sN, List<AcceptedValueCL> acceptedCLValues) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.acceptedValueCLS = acceptedCLValues;
    }

    @Override
    public String toString() {
        return "PrepareOkMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", acceptedValues=" + acceptedValueCLS +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<PrepareOkMsg>() {
        public void serialize(PrepareOkMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            out.writeInt(msg.acceptedValueCLS.size());
            for (AcceptedValueCL v : msg.acceptedValueCLS) {
                v.serialize(out);
            }
        }

        public PrepareOkMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            int acceptedCLValuesLength = in.readInt();
            List<AcceptedValueCL> acceptedCLValues = new ArrayList<>(acceptedCLValuesLength);
            for (int i = 0; i < acceptedCLValuesLength; i++) {
                acceptedCLValues.add(AcceptedValueCL.deserialize(in));
            }
            return new PrepareOkMsg(instanceNumber, sN, acceptedCLValues);
        }
    };
}
