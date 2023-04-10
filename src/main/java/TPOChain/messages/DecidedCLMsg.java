package TPOChain.messages;

import TPOChain.utils.AcceptedValue;
import TPOChain.utils.AcceptedValueCL;
import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class DecidedCLMsg extends ProtoMessage {

    public static final short MSG_CODE = 303;

    public final int iN;

    public final Host node;
    public final int NodeiN;

    public final SeqN sN;

    public final List<AcceptedValueCL> decidedValues;

    public DecidedCLMsg(int iN,Host node,int NodeiN, SeqN sN, List<AcceptedValueCL> decidedValues) {
        super(MSG_CODE);

        this.iN = iN;

        this.node=node;
        this.NodeiN=NodeiN;

        this.sN = sN;
        this.decidedValues = decidedValues;
    }

    @Override
    public String toString() {
        return "DecidedMsg{" +
                "iN=" + iN +
                "node=" + node +
                "NodeiN=" + NodeiN +
                ", sN=" + sN +
                ", decidedValues=" + decidedValues +
                '}';
    }


    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<DecidedCLMsg>() {
        public void serialize(DecidedCLMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            Host.serializer.serialize(msg.node,out);
            out.writeInt(msg.NodeiN);
            msg.sN.serialize(out);
            out.writeInt(msg.decidedValues.size());
            for (AcceptedValueCL v : msg.decidedValues)
                v.serialize(out);
        }

        public DecidedCLMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            Host t=Host.serializer.deserialize(in);
            int  NodeiN=in.readInt();
            SeqN seqN = SeqN.deserialize(in);
            int decidedValuesLength = in.readInt();
            List<AcceptedValueCL> decidedValues = new ArrayList<>(decidedValuesLength);
            for (int i = 0; i < decidedValuesLength; i++)
                decidedValues.add(AcceptedValueCL.deserialize(in));
            return new DecidedCLMsg(instanceNumber,t,NodeiN ,seqN, decidedValues);
        }
    };
}