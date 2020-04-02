

public class CompactElement {
    
    int oldValue;
    int newValue;
    Transaction desc;

    CompactElement() {

    }

    CompactElement(CompactElement parent) {
        this.oldValue = parent.oldValue;
        this.newValue = parent.newValue;
        this.desc = parent.desc;
    }


    @Override
    public String toString() {

        return "oldValue: " + oldValue + "\t\t\tnewValue: " + newValue;
    }

    
    public CompactElement(int oldV, int newV, Transaction t) {
        this.oldValue = oldV;
        this.newValue = newV;
        this.desc = t;
    }
}
