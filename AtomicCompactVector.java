import java.util.concurrent.atomic.AtomicReferenceArray;

public class AtomicCompactVector {

    AtomicReferenceArray<CompactElement> array;
    public AtomicCompactVector() {

        this.array = new AtomicReferenceArray<>(new CompactElement[10]);
    }

}
