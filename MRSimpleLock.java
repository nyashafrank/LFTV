import java.util.BitSet;
import java.util.concurrent.atomic.AtomicReference;

public class MRSimpleLock {

    AtomicReference<BitSet> bits = new AtomicReference<BitSet>();

    public MRSimpleLock(){
       
        BitSet b = new BitSet(64);
        bits.set(b);
    }


    public void lock(BitSet resources){

        for(;;){

            // Grab the current bits
            BitSet bitsRef = bits.get();

            // resources & bitsRef 
            BitSet resourcesANDbitsRef = (BitSet) resources.clone();
            resourcesANDbitsRef.and(bitsRef);

            // Check that there's no clashing bit request
            if(resourcesANDbitsRef.isEmpty()){

                // resources | bitsRef 
                BitSet resourcesORbitsRef = (BitSet) resources.clone();
                resourcesORbitsRef.or(bitsRef);

                if(bits.compareAndSet(bitsRef, resourcesORbitsRef)){
                    break;
                }
            }
        }
    }

    public void unlock(BitSet resources){

        for(;;){

            BitSet bitsRef = bits.get();

            // bits & ~resources
            BitSet bitsANDNOTresources = (BitSet) bitsRef.clone();
            bitsANDNOTresources.andNot(resources);

            if(bits.compareAndSet(bitsRef, bitsANDNOTresources)){
               // System.out.println(Thread.currentThread().getName() + " request " + resources + " unlocked\n");
                break;
            }
        }
    }
}