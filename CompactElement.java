

public class CompactElement {
    
    int oldValue;
    int newValue;
    Transaction desc;

    CompactElement() {

    }

    CompactElement(int oldValue, int newValue, Transaction desc) {
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.desc = desc;
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
}
