package common.values;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.Objects;

public class SortItem {
    private  final Host node;
    private  final int  iN;

    public SortItem(Host node,int iN) {
        this.node=node;
        this.iN=iN;
    }
    public Host getNode() {
        return node;
    }

    public int getiN() {
        return iN;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SortItem)) return false;
        SortItem that = (SortItem) o;
        return (node.equals(that.getNode()) && (iN==that.getiN()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(node,iN);
    }
}
