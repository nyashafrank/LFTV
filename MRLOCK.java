import java.util.BitSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

//import sun.tools.tree.BooleanExpression;

public class MRLOCK {
    class Cell {
        AtomicReference<Integer> seq;
        BitSet bits;
    }

    Cell[] buffer;
    Integer mask;
    AtomicReference<Integer> head;
    AtomicReference<Integer> tail;

    public MRLOCK(Integer size) {

        size = 2;
        while(size <= 2038 ) {
            size = size << 1;
        }
        

        buffer = new Cell[size];
        mask = size - 1;
        
        // memory_order_relaxed ?
        head = new AtomicReference<Integer>(0);
        tail = new AtomicReference<Integer>(0);

        for (int i = 0; i < size; i++) {
            buffer[i] = new Cell();
            buffer[i].bits = new BitSet();
            buffer[i].bits.set(0, 64);
            buffer[i].seq = new AtomicReference<Integer>(i);
        }
    }

    public Integer lock(BitSet r) {
        Cell c;
        Integer pos;

        for(;;) {
            pos = tail.get();
            c = buffer[pos & mask];
            Integer seq = c.seq.get();
            Integer dif = seq - pos;
            if (dif == 0) {
                if (Objects.equals(tail.compareAndExchange(pos, pos+1), pos)) {
                    //System.out.println("pos = " + pos + "\nseq = " + seq + "\n dif = " + dif);
                    break;
                }
            }    
            printRequests();

           // System.out.println(" STUCK HERE");
           // System.out.println("pos = " + pos + "\nseq = " + seq + "\n dif = " + dif);
        }
//        System.out.println("Not Stuck Anymore");

        c.bits = r;
        c.seq.set(pos + 1);
        Integer spin = head.get();
        while (!Objects.equals(spin, pos)) {
            // the cell is free and recycled OR the request in the cell has no conflict, then advance down the line
            if ( !isConflict(r, buffer[spin & mask].bits) || pos - buffer[spin & mask].seq.get() > mask ) {
                spin++;
//                System.out.println("In if statement");
//                System.out.println("spin: " + spin + ", pos" + pos);
            }
                
//             System.out.println("In while statement");
                
        }
        
//        System.out.println("While Loop Over");

        return pos;
    }


    public Boolean isConflict(BitSet set1, BitSet set2 ) {

        BitSet b = (BitSet) set2.clone();

        b.and(set1);

        if(b.isEmpty())
            return false;

        else return true;
    }

    public void unlock(Integer h) {
//        System.out.println("Unlock happening");
        // clear the cell request at index h 
        buffer[h & mask].bits.clear();

        Integer pos = head.get();

        // while the head's cell request is clear
        while(buffer[pos & mask].bits.isEmpty()) {

            System.out.println( Thread.currentThread().getName() +" unlocking");
            Cell c = buffer[pos & mask];

            Integer seq = c.seq.get();
            Integer dif = seq - (pos + 1);

            if(dif == 0) {
                if(head.compareAndSet(pos, pos + 1)) {
                    // reset and recycle the cell
                    c.bits.set(0, 64);
                    c.seq.set(pos+mask +1);
                }
            }

//            System.out.println("WE stuck");
            pos = head.get();
        }

       
        //System.out.println("pos = " + pos + "\nseq = " + c.seq.get() + "\n dif = " + dif);
        

    }

    public void printRequests() {

        for(int temp = head.get(); temp != tail.get(); temp++)
            System.out.println(buffer[temp].bits);

        System.out.println();
    }
}