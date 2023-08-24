package TPOChain.messages;

import TPOChain.utils.AcceptedValue;
import TPOChain.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;


public class QueryOldChannelUserOkMsg extends ProtoMessage{
    
    public static final short MSG_CODE = 702;
    
    public final SeqN sN;
    public List<AbstractMap.SimpleEntry<Host, List<AcceptedValue>>> answerList;
   
    //public  List<AcceptedValue> acceptedValue;
    public QueryOldChannelUserOkMsg(SeqN sN, List<AbstractMap.SimpleEntry<Host, List<AcceptedValue>>> _ans) {
        super(MSG_CODE);
        this.sN = sN;
        answerList=_ans;
        //this.acceptedValue = acceptedCLValues;
    }

    @Override
    public String toString() {
        return "PrepareOkMsg{" +
                //"iN=" + iN +
                ", answerList=" + answerList.toString() +
              //  ", acceptedValues=" + acceptedValue +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<QueryOldChannelUserOkMsg>() {
        public void serialize(QueryOldChannelUserOkMsg msg, ByteBuf out) throws IOException {
            //out.writeInt(msg.iN);
            msg.sN.serialize(out);
            
            out.writeInt(msg.answerList.size());
            for (AbstractMap.SimpleEntry<Host, List<AcceptedValue>> tempEntry:msg.answerList) {
                Host.serializer.serialize(tempEntry.getKey(), out);
                out.writeInt(tempEntry.getValue().size());
                for (AcceptedValue v : tempEntry.getValue()) {
                    v.serialize(out);
                }
            }
        }

        public QueryOldChannelUserOkMsg deserialize(ByteBuf in) throws IOException {
            //int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            
            int listlen=in.readInt();
            List<AbstractMap.SimpleEntry<Host, List<AcceptedValue>>> _ansList=new ArrayList<>(listlen);
            for (int j=0;j<listlen;j++){
                Host node = Host.serializer.deserialize(in);
                
                int acceptedValuesLength = in.readInt();
                List<AcceptedValue> acceptedValues = new ArrayList<>(acceptedValuesLength);
                for (int i = 0; i < acceptedValuesLength; i++) {
                    acceptedValues.add(AcceptedValue.deserialize(in));
                }
                
                _ansList.add(new AbstractMap.SimpleEntry<>( node ,acceptedValues) );
            }
            return new QueryOldChannelUserOkMsg(sN, _ansList);
        }
    };
}