import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

//import org.graalvm.compiler.nodes.calc.IntegerDivRemNode.Op;

public class CompactLFTV {
    private class BucketAndIndex {
        int bucket;
        int indexInBucket;
        BucketAndIndex(int bucket, int indexInBucket) {
            this.bucket = bucket;
            this.indexInBucket = indexInBucket;
        }
    }
    public static void main(String[] args) {
        CompactLFTV test = new CompactLFTV();
        test.Populate(1000);
    }

    private static final int FIRST_BUCKET_CAPACITY = 8;
    private static final int BUCKETS_LENGTH = 30;
    private static final int UNSET = Integer.MAX_VALUE;
    public AtomicReference<CompactElement> size;
    private final AtomicReferenceArray<AtomicReferenceArray<CompactElement>> buckets;

    public CompactLFTV() {
        size = new AtomicReference<CompactElement>(new CompactElement(0,0, new Transaction(TxnStatus.committed)));
        buckets = new AtomicReferenceArray<AtomicReferenceArray<CompactElement>>(BUCKETS_LENGTH);
        buckets.set(0, new AtomicReferenceArray<CompactElement>(FIRST_BUCKET_CAPACITY));
    }

    public void Populate(int amount) {
        Reserve(amount);
        Transaction t = new Transaction(TxnStatus.committed);
        ConcurrentHashMap<Integer, RWOperation> rwset = new ConcurrentHashMap<>();
        t.set.set(rwset);
        
        Random random = new Random();
        for (int i = 0; i < amount; i++) {
            BucketAndIndex bai = calculateWhichBucketAndIndex(i);
            CompactElement c = new CompactElement();
            c.oldValue = Integer.MAX_VALUE;
            c.newValue = random.nextInt(50);
            c.desc = t;

            Operation op = new Operation(OperationType.pushBack, c.newValue, i);

            RWOperation rwop = new RWOperation();
            rwop.lastWriteOp = op;

            rwset.put(i, rwop);

            buckets.get(bai.bucket).compareAndSet(bai.indexInBucket, null, c);
        }

        CompactElement currentSize = size.get();
        CompactElement newSize = new CompactElement(currentSize);
        newSize.newValue = amount;
        size.compareAndExchange(currentSize, newSize);
    }

    public void Populate() {
        CompactElement currentSize = size.get();
        CompactElement newSize = new CompactElement(currentSize);
        int s = currentSize.newValue;

        
        BucketAndIndex bai = calculateWhichBucketAndIndex(s);
        if(buckets.get(bai.bucket).get(bai.indexInBucket) == null) {
           System.out.println("Here!!!");

            Transaction t = new Transaction(TxnStatus.committed);


            t.operations = new Operation[1];
            int v = ThreadLocalRandom.current().nextInt(50);
            Operation op = new Operation(OperationType.pushBack, v, s);
            t.operations[0] = op;

            RWOperation rwop = new RWOperation();
            rwop.lastWriteOp = op;

            ConcurrentHashMap<Integer, RWOperation> rwset = new ConcurrentHashMap<>();
            t.set.set(rwset);
            t.set.get();
            rwset.put(s, rwop);
            t.set.getAndSet(rwset);

            CompactElement c = new CompactElement();
            c.oldValue = Integer.MAX_VALUE;
            c.newValue = v;
            c.desc = t;
            
            newSize.newValue = s+1;
            size.compareAndExchange(currentSize, newSize);

            buckets.get(bai.bucket).compareAndSet(s, null, c);
    
        }
        
    }

    public BucketAndIndex calculateWhichBucketAndIndex(int index) {
        // Account for initial capacity being 8 instead of 0
        int x = index + FIRST_BUCKET_CAPACITY;

        // Each leading bit difference between x and the first bucket's capacity increases the 
        // bucket index by 1 since each bucket capacity is 2 times the previous buckets capacity
        int bucket = Integer.numberOfLeadingZeros(FIRST_BUCKET_CAPACITY) - Integer.numberOfLeadingZeros(x);

        // xor to trim x to be within the capacity of the correct bucket
        int indexInBucket = Integer.highestOneBit(x) ^ x;
        return new BucketAndIndex(bucket, indexInBucket);
    }

    public boolean UpdateElem(int index, CompactElement newElem) {
        BucketAndIndex bucketAndIndex = calculateWhichBucketAndIndex(index);
        CompactElement oldElem;
        AtomicReferenceArray<CompactElement> bucket;
        RWOperation op;

        //System.out.println("Grabbing index " + index);
        do {
            bucket = buckets.get(bucketAndIndex.bucket);
            if (bucket == null) {
                newElem.desc.status.set(TxnStatus.aborted);
                return false;
            }
            oldElem = bucket.get(bucketAndIndex.indexInBucket);
            if(oldElem == null) {
                newElem.desc.status.set(TxnStatus.aborted);
                return false;
            }

            if (newElem.desc.status.get() != TxnStatus.active) {
                return true;
            }

            if (oldElem.desc == newElem.desc) {
                return true;
            }

            while (oldElem.desc.status.get() == TxnStatus.active) {
                //completeTransaction(oldElem.desc, index)
            }

            if (oldElem.desc.status.get() == TxnStatus.committed && oldElem.desc.set.get().get(index).lastWriteOp != null) {
                newElem.oldValue = oldElem.newValue;
            } else {
                newElem.oldValue = oldElem.oldValue;
            }

            op = newElem.desc.set.get().get(index);
            if (op.checkBounds == true && newElem.oldValue == UNSET) {
                newElem.desc.status.set(TxnStatus.aborted);
                return false;
            }

        } while (!buckets.get(bucketAndIndex.bucket).compareAndSet(bucketAndIndex.indexInBucket, oldElem, newElem));

        op = newElem.desc.set.get().get(index);
        for (int i = 0; i < op.readList.size(); i++) {
            op.readList.get(i).returnValue = newElem.oldValue;
        }

        return true;
    }


    // Read an element from shared memory - For reads not followed by a write
    public int ReadElement(int index) {

        BucketAndIndex bai = calculateWhichBucketAndIndex(index);
        CompactElement c = buckets.get(bai.bucket).get(bai.indexInBucket);

        if(c == null) {
            System.err.println("The element is null in readElement()");
            return UNSET;
        }
        if(c.desc.status.get() == TxnStatus.committed)
            return c.newValue;
        else
            return c.oldValue;
    }

    public void PrintVector() {

        for(int i = 0; i < size.get().newValue; i++) {
            CompactElement c = buckets.get(0).get(i);
            System.out.println(c);
        }
    }

    public void Reserve(int newCapacity) {
        int capacity = FIRST_BUCKET_CAPACITY;
        int totalCapacity = capacity;
        int index = 0;
        while (newCapacity > totalCapacity) {
            index++;
            capacity *= 2;
            totalCapacity += capacity;
            buckets.compareAndSet(index, null, new AtomicReferenceArray<CompactElement>(capacity));
        }
    }

}