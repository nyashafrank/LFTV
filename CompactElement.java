

public class CompactElement {
    
    int oldValue;
    int newValue;
    Transaction desc;


    @Override
    public String toString() {

        return "oldValue: " + oldValue + "\t\t\tnewValue: " + newValue;
    }


    public CompactElement() {

    }
    
    public CompactElement(int oldV, int newV, Transaction t) {
        this.oldValue = oldV;
        this.newValue = newV;
        this.desc = t;
    }
}
