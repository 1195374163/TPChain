package common.values;


import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// leader发送的排序消息      
public class SortValue extends PaxosValue {

    //private   Host node;
    //private   int  iN;
    //public SortValue(Host node,int iN) {
    //    super(Type.SORT);
    //    this.node=node;
    //    this.iN=iN;
    //}
    //public Host getNode() {
    //    return node;
    //}
    //public int getiN() {
    //    return iN;
    //}
    //
    
    // 使用List集
    public List<SortItem>  sortItemsbatch;
    public SortValue() {
        super(Type.SORT);
        sortItemsbatch=new ArrayList<>();
    }
    public SortValue(List<SortItem>  list) {
        super(Type.SORT);
        sortItemsbatch=list;
    }

    public List<SortItem> getSortItemsbatch() {
        return sortItemsbatch;
    }

    

    @Override
    public String toString() {
        return "SortValue{" +
                sortItemsbatch.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SortValue)) return false;
        SortValue that = (SortValue) o;
        //return (sortItemsbatch, && (iN==that.getiN()));
        return Objects.equals(sortItemsbatch,that.sortItemsbatch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortItemsbatch);
    }
    
    //父类paxosvalue会使用
    static ValueSerializer serializer = new ValueSerializer<SortValue>() {
        @Override
        public void serialize(SortValue sort_value, ByteBuf out) throws IOException {
            int size=sort_value.sortItemsbatch.size();
            out.writeInt(size);
            for (int i=0;i<size;i++){
                Host.serializer.serialize(sort_value.sortItemsbatch.get(i).getNode(),out);
                out.writeInt(sort_value.sortItemsbatch.get(i).getiN());
            }
        }

        @Override
        public SortValue deserialize(ByteBuf in) throws IOException {
            int size=in.readInt();
            List<SortItem> temp=new ArrayList<>();
            for (int i=0;i<size;i++){
                Host _node=Host.serializer.deserialize(in);
                int  _iN= in.readInt();
                temp.add(new SortItem(_node,_iN));
            }
            return new SortValue(temp);
        }
    };
}
