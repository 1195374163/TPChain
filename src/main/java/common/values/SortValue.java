package common.values;

import frontend.ops.OpBatch;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.Objects;

//TODO leader发送的排序消息      
public class SortValue extends PaxosValue {
    
    private  final Host node;
    private  final   int  iN;
    
    public SortValue(Host node,int iN) {
//        super(Type.SORT);
        super(Type.SORT);
        this.node=node;
        this.iN=iN;
    }

    public Host getnode() {
        return node;
    }

    public int getiN() {
        return iN;
    }
    

    @Override
    public String toString() {
        return "SortValue{" +
                "node=" + node +
                "iN=" + iN +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SortValue)) return false;
        SortValue that = (SortValue) o;
        return (node.equals(that.getnode()) && (iN==that.getiN()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(node,iN);
    }

    static ValueSerializer serializer = new ValueSerializer<SortValue>() {
        @Override
        public void serialize(SortValue sort_value, ByteBuf out) throws IOException {
            Host.serializer.serialize(sort_value.getnode(),out);
            out.writeInt(sort_value.getiN());
        }

        @Override
        public SortValue deserialize(ByteBuf in) throws IOException {
            Host _node=Host.serializer.deserialize(in);
            int  _iN= in.readInt();
            return new SortValue(_node,_iN);
        }
    };
}
