import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class CompactLFTV {
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

    public Boolean CompleteTransaction(Transaction desc, int lowerBounds) {
        boolean result = true;

        Set<Integer> indexes = desc.set.get().keySet();

        List<Integer> list = new ArrayList<>();
        for(Integer x : indexes) {
            if (x >= lowerBounds)
                list.add(x);
        }
        
        // Complete in ascending order to prevent conflict
        Collections.sort(list);

        Iterator<Integer> it = list.iterator();        
        while(it.hasNext()) {
            int index = it.next();
            RWOperation rwop = desc.set.get().get(index);

            // If there are only read operations on an index
            if(rwop.lastWriteOp == null && rwop.readList.size() > 0) {
                // Get the value from shared memory
                int retval = ReadElement(index);
                for(int i = 0; i < rwop.readList.size(); i++) {
                    rwop.readList.get(i).returnValue = retval;
                }
            } else {
                CompactElement newElem = new CompactElement();
                newElem.desc = desc;
                newElem.newValue = rwop.lastWriteOp.value;
                result = UpdateElem(index, newElem);

                if(!result) {
                    return false;
                }
            }
        }

        desc.status.set(TxnStatus.committed);
        return true;
    }

    public boolean Preprocess(Transaction t) {

        ConcurrentMap<Integer,RWOperation> localSet = new ConcurrentHashMap<Integer, RWOperation>();
        CompactElement oldSize = null;
        CompactElement newSize = null;

        RWOperation rwop = null;
        int largestReserve = 0;
        int possibleSize = 0;
        boolean sizeAcquired = false;        

        for (int i = 0; i < t.operations.length; i++) {   
            Operation op = t.operations[i];

            if(localSet.containsKey(op.index)) 
                rwop = localSet.get(op.index);
            else    
                rwop = new RWOperation();


            // READ OPERATION
            if(op.operationType == OperationType.read) {

                if(sizeAcquired && op.index >= possibleSize) {
                    return false;
                }
                
                // Add read to the list so we can store the elements oldval in its return in updateElement
                if(rwop.lastWriteOp == null ){

                    // Check if it's the first operation for this index and set checkBounds
                    if(rwop.readList.size() == 0) {
                        rwop.checkBounds = true;
                    }

                    rwop.readList.add(op);
                    localSet.put(op.index, rwop);
                }
                else {
                    op.returnValue = rwop.lastWriteOp.value;
                }
            }


            // WRITE OPERATION
            else if (op.operationType == OperationType.write) {

                // Check if the index is greater than the possible size
                if(sizeAcquired && op.index >= possibleSize) {
                    return false;
                }

                // Check if it's the first operation for this index and set checkBounds
                if(rwop.lastWriteOp == null && rwop.readList.size() == 0) {
                    rwop.checkBounds = true;
                }

                rwop.lastWriteOp = op;
                localSet.put(op.index, rwop);
            }


            // SIZE OPERATION
            else if (op.operationType == OperationType.size) {
                if (!sizeAcquired) {
                    do {
                        oldSize = size.get();
                        newSize = new CompactElement(oldSize.oldValue, oldSize.newValue, t);

                        if (oldSize.desc == t) {
                            break;
                        }
                        
                        while (oldSize.desc.status.get() == TxnStatus.active) {
                            boolean success = Preprocess(oldSize.desc);
                            if(success) CompleteTransaction(oldSize.desc, 0);
                        } 

                        if (oldSize.desc.status.get() == TxnStatus.committed)
                                newSize.oldValue = oldSize.newValue;
                        else 
                                newSize.oldValue = oldSize.oldValue;

                         newSize.desc = t;
                    } while(!size.compareAndSet(oldSize, newSize));

                    possibleSize = newSize.oldValue;
                    sizeAcquired = true;    
                }

                op.returnValue = possibleSize;
            }

            // POPBACK OPERATION
            else if (op.operationType == OperationType.popBack) {
                if(!sizeAcquired) {
                    do {
                        oldSize = size.get();
                        newSize = new CompactElement(oldSize.oldValue, oldSize.newValue, t);

                        if (oldSize.desc == t) {
                            break;
                        }
                        
                        while (oldSize.desc.status.get() == TxnStatus.active) {
                            boolean success = Preprocess(oldSize.desc);
                            if(success) CompleteTransaction(oldSize.desc, 0);
                        } 

                        if (oldSize.desc.status.get() == TxnStatus.committed)
                                newSize.oldValue = oldSize.newValue;
                        else 
                                newSize.oldValue = oldSize.oldValue;

                         newSize.desc = t;
                    } while(!size.compareAndSet(oldSize, newSize));

                    sizeAcquired = true;
                    possibleSize = newSize.oldValue;
                }

                possibleSize--;
                if(localSet.containsKey(possibleSize)) 
                    rwop = localSet.get(possibleSize);
                else    
                    rwop = new RWOperation();

                if(rwop.lastWriteOp == null && rwop.readList.size() == 0) {
                    rwop.checkBounds = false;
                }

                op.index = possibleSize;
                rwop.readList.add(op);
                rwop.lastWriteOp = op;
                localSet.put(op.index, rwop);
            }


            // PUSHBACK OPERATION
            else if (op.operationType == OperationType.pushBack) {
                if(!sizeAcquired) {
                    do {
                        oldSize = size.get();
                        newSize = new CompactElement(oldSize.oldValue, oldSize.newValue, t);

                        if (oldSize.desc == t) {
                            break;
                        }
                        
                        while (oldSize.desc.status.get() == TxnStatus.active) {
                            boolean success = Preprocess(oldSize.desc);
                            if(success) CompleteTransaction(oldSize.desc, 0);
                        } 

                        if (oldSize.desc.status.get() == TxnStatus.committed)
                                newSize.oldValue = oldSize.newValue;
                        else 
                                newSize.oldValue = oldSize.oldValue;

                        newSize.desc = t;
                    } while(!size.compareAndSet(oldSize, newSize));

                    sizeAcquired = true;
                    possibleSize = newSize.oldValue;
                }

                if(localSet.containsKey(possibleSize)) 
                    rwop = localSet.get(possibleSize);
                 else    
                    rwop = new RWOperation();

                if(rwop.lastWriteOp == null && rwop.readList.size() == 0) {
                    rwop.checkBounds = false;
                }

                rwop.lastWriteOp = op;
                localSet.put(possibleSize, rwop);
                possibleSize++;
                largestReserve = Math.max(largestReserve, possibleSize);
            }


            // Keep track of largest reserve call
            if(op.operationType == OperationType.reserve) {
                if(op.index > largestReserve)
                    largestReserve = op.index;
            }
        }

        // Reserve the amount needed 
        if(largestReserve > 0) {
            Reserve(largestReserve);
        }

        if(sizeAcquired) {
            if (newSize != null)
                newSize.newValue = possibleSize;
        }
        // Attempt to save the preprocessed set that CompleteTransaction will use 
        t.set.compareAndSet(null, (ConcurrentHashMap<Integer, RWOperation>) localSet);

        return true;
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


    /*******************
     *  HELPER METHODS *
     *******************/                 

    private class BucketAndIndex {
        int bucket;
        int indexInBucket;
        BucketAndIndex(int bucket, int indexInBucket) {
            this.bucket = bucket;
            this.indexInBucket = indexInBucket;
        }
    }

    private BucketAndIndex calculateWhichBucketAndIndex(int index) {
        // Account for initial capacity being 8 instead of 0
        int x = index + FIRST_BUCKET_CAPACITY;

        // Each leading bit difference between x and the first bucket's capacity increases the 
        // bucket index by 1 since each bucket capacity is 2 times the previous buckets capacity
        int bucket = Integer.numberOfLeadingZeros(FIRST_BUCKET_CAPACITY) - Integer.numberOfLeadingZeros(x);

        // xor to trim x to be within the capacity of the correct bucket
        int indexInBucket = Integer.highestOneBit(x) ^ x;
        return new BucketAndIndex(bucket, indexInBucket);
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
        newSize.desc = t;
        newSize.newValue = amount;
        size.compareAndExchange(currentSize, newSize);
    }




    // Update the element at the given index with the new element
    private boolean UpdateElem(int index, CompactElement newElem) {
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
                CompleteTransaction(oldElem.desc, index);
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
           // System.err.println("The element is null in readElement()");
            return UNSET;
        }
        if(c.desc.status.get() == TxnStatus.committed)
            return c.newValue;
        else
            return c.oldValue;
    }


    public void PrintVector() {

        for(int i = 0; i < size.get().newValue; i++) {
          //  System.out.println("printing index " + i);
            BucketAndIndex bai = calculateWhichBucketAndIndex(i);
            CompactElement c = buckets.get(bai.bucket).get(bai.indexInBucket);
            if(c != null)
                System.out.println(c);
        }
    }


    public boolean UpdateElemNoHelping(int index, CompactElement newElem) {
        BucketAndIndex bucketAndIndex = calculateWhichBucketAndIndex(index);
        CompactElement oldElem;
        AtomicReferenceArray<CompactElement> bucket;
        RWOperation op;

        bucket = buckets.get(bucketAndIndex.bucket);
        if (bucket == null) {
            newElem.desc.status.set(TxnStatus.aborted);
            return false;
        }

        oldElem = bucket.get(bucketAndIndex.indexInBucket);
        if(oldElem == null) {
            newElem.oldValue = UNSET;
           if( buckets.get(bucketAndIndex.bucket).compareAndSet(bucketAndIndex.indexInBucket, null, newElem)){
               
                CompactElement c = buckets.get(bucketAndIndex.bucket).get(bucketAndIndex.indexInBucket);
               // System.out.println("Swapped out old elem \n "+ c);
           }
        }

        else{

            if (oldElem.desc.status.get() == TxnStatus.committed && oldElem.desc.set.get().get(index).lastWriteOp != null) {
                newElem.oldValue = oldElem.newValue;
            } else {
                newElem.oldValue = oldElem.oldValue;
            }

            op = newElem.desc.set.get().get(index);
            if (op.checkBounds == true && newElem.oldValue == UNSET) {
                newElem.desc.status.set(TxnStatus.aborted);
               // System.out.println("unset val");
                return false;
            }

            if(buckets.get(bucketAndIndex.bucket).compareAndSet(bucketAndIndex.indexInBucket, oldElem, newElem)){
                //System.out.println("Element successfully replaced");
            }
        }

        op = newElem.desc.set.get().get(index);
        for (int i = 0; i < op.readList.size(); i++) {
            op.readList.get(i).returnValue = newElem.oldValue;
        }

        return true;
    }
}