
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;




public class lockfree {

    public static int totalOpTypes = 6;
    public static final int UNSET = Integer.MAX_VALUE;
    public static OperationType[] opTypeVals = OperationType.values();

    public static CompactLFTV lftv = new CompactLFTV();

    public static void main(String[] args) {

        System.out.println("\t------ Initial State ------\n");
        // Prepopulate compact LFTV with pushback operations;
        for(int i = 0; i < 6; i++) {                            // ******** MOVE THE LOOP INTO POPULATE METHOD AND CHANGE 6 TO LARGER NUMBER *********
            lftv.Populate();
        }

        lftv.PrintVector();

        // Initialize threads array
        Thread[] threads = new Thread[3];           // ******** VARY NUMBER OF THREADS *********

        // Create Threads
        for(int i = 0; i < threads.length; i++)
        {
            Transaction[] transactions = BuildTransactions();
            Thread t = new Thread(new Perform(transactions, lftv));       // ******** PASS IN LFTV REFERENCE TOO *********

            t.setName(String.valueOf(i+1));
            threads[i] = t;
        }

        // Start timer
        long startTime = System.currentTimeMillis();

        // Start threads
        for(int i = 0; i < threads.length; i++)
            threads[i].start();

        // Join threads
        try {
            for (Thread thread : threads) 
                thread.join();
        } catch(InterruptedException e){
            System.out.println("Threads interrupted");
        }

        System.out.println("\n\n\t------ Final State ------\n");
        lftv.PrintVector();
        
        long endTime = System.currentTimeMillis();
        System.out.println("\nExecution time = " + (endTime - startTime) + "ms\n");
    }



    // Creates an array of transactions for a thread to pull from
    public static Transaction[] BuildTransactions() {

        Transaction[] transactions = new Transaction[3];        // ******** CHANGE THIS NUMBER LATER *********

        // Build each transaction and insert it into transactions array
        for(int x = 0; x < transactions.length; x++) {

            int value;
            int operationCount = 0;
            Operation[] operations = new Operation[5];
    
            // Insert 5 random operations per transaction
            while(operationCount < 5) {

                // get random operation type
                double ratio = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
                OperationType opType = GetOperationType(ratio);
    
                // Popback's value field should always be max integer
                if(opType == OperationType.popBack)
                    value = Integer.MAX_VALUE;
                    
                // get random value to write or push
                else
                    value = ThreadLocalRandom.current().nextInt(100);
    
                // choose random index to perform operation on in vector
                int vectorIndex = ThreadLocalRandom.current().nextInt(6);         // ******** CHANGE THE BOUND *********
    
                Operation operation = new Operation(opType, value, vectorIndex);
                operations[operationCount] = operation;
    
                operationCount++;
            }

            Transaction t = new Transaction(operations);

            transactions[x] = t;
        }

        return transactions;
    }



    // Returns an operation based off the ratio
    public static OperationType GetOperationType(double ratio) {

        // Read operation - 20%                                         // ******** VARY THE RATIOS *********
        if(ratio < 0.5)
            return opTypeVals[0];
    
        // Write operation - 20%
        else if (ratio >= 0.5 && ratio < 1.0)
            return opTypeVals[1];
        
        // Pushback - 20%
        else if (ratio >= 0.4 && ratio < 0.6)
            return opTypeVals[2];

        // Popback - 20%
        else if (ratio >= 0.6 && ratio < 0.8)
            return opTypeVals[3];

        // Size - 10%
        else if (ratio >= 0.8 && ratio < 0.9)
            return opTypeVals[4];

        // Reserve - 10%
        else
            return opTypeVals[5];
    }
}





class Perform implements Runnable {

    public Transaction[] transactions;
    public CompactLFTV v;
    public CompactElement possibleSize;
    public CompactElement oldSize;
    public int vectorSize;
    CompactElement newSize;

   


    public Perform (Transaction[] t, CompactLFTV lftv) {              // ******** PASS IN LFTV REFERENCE  *********
        this.transactions = t;
        this.v = lftv;

        //possibleSize = new CompactElement();
    }


    public void run() {

        // for(int x = 0; x < transactions.length; x++) 
        //     System.out.println("Thread " + Thread.currentThread().getName() + "\n" + transactions[x] + "\n");

        for(int i = 0; i < transactions.length; i++) {

            Transaction t = transactions[i];
            t.set = null;

            boolean success = Preprocess(t);

            if(success)
                CompleteTransaction(t, 0);
            else 
                t.status.set(TxnStatus.aborted);

           // System.out.println("Thread " + Thread.currentThread().getName() + "\n" + t);

           // v.PrintVector();


        }
    }


    private boolean Preprocess(Transaction t) {

        ConcurrentMap<Integer,RWOperation> localSet = new ConcurrentHashMap<Integer, RWOperation>();

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
                        oldSize = v.size.get();
                        newSize = new CompactElement(oldSize.oldValue, oldSize.newValue, t);
                        
                        while (oldSize.desc.status.get() == TxnStatus.active)
                             CompleteTransaction(oldSize.desc, 0);

                        if (oldSize.desc.status.get() == TxnStatus.committed)
                                newSize.oldValue = oldSize.newValue;
                        else 
                                newSize.oldValue = oldSize.oldValue;

                         newSize.desc = t;
                    } while(!v.size.compareAndSet(oldSize, newSize));

                    possibleSize = newSize.oldValue;
                    sizeAcquired = true;    
                }

                op.returnValue = possibleSize;
            }


            // POPBACK OPERATION
            else if (op.operationType == OperationType.popBack) {

                // Check if it's the first operation for this index and set checkBounds
                if(rwop.lastWriteOp == null && rwop.readList.size() == 0) {
                    rwop.checkBounds = false;
                }

                if(!sizeAcquired) {
                    do {
                        oldSize = v.size.get();
                        newSize = new CompactElement(oldSize.oldValue, oldSize.newValue, t);
                        
                        while (oldSize.desc.status.get() == TxnStatus.active)
                             CompleteTransaction(oldSize.desc, 0);

                        if (oldSize.desc.status.get() == TxnStatus.committed)
                                newSize.oldValue = oldSize.newValue;
                        else 
                                newSize.oldValue = oldSize.oldValue;

                         newSize.desc = t;
                    } while(!v.size.compareAndSet(oldSize, newSize));

                    sizeAcquired = true;
                    possibleSize = newSize.oldValue;
                }

                possibleSize--;
                if(localSet.containsKey(possibleSize)) 
                    rwop = localSet.get(possibleSize);
                else    
                    rwop = new RWOperation();

                op.index = possibleSize;
                rwop.readList.add(op);
                rwop.lastWriteOp = op;
                localSet.put(op.index, rwop);
            }


            // PUSHBACK OPERATION
            else if (op.operationType == OperationType.pushBack) {

                // Check if it's the first operation for this index and set checkBounds
                if(rwop.lastWriteOp == null && rwop.readList.size() == 0) {
                    rwop.checkBounds = false;
                }

                if(!sizeAcquired) {
                    do {
                        oldSize = v.size.get();
                        newSize = new CompactElement(oldSize.oldValue, oldSize.newValue, t);
                        
                        while (oldSize.desc.status.get() == TxnStatus.active)
                            CompleteTransaction(oldSize.desc, 0);

                        if (oldSize.desc.status.get() == TxnStatus.committed)
                                newSize.oldValue = oldSize.newValue;
                        else 
                                newSize.oldValue = oldSize.oldValue;

                        newSize.desc = t;
                    } while(!v.size.compareAndSet(oldSize, newSize));

                    sizeAcquired = true;
                    possibleSize = newSize.oldValue;
                }

                if(localSet.containsKey(possibleSize)) 
                    rwop = localSet.get(possibleSize);
                 else    
                    rwop = new RWOperation();

                rwop.checkBounds = false;
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

        // do something with the largestReserve value
        if(largestReserve > 0) {
                //v.Reserve(largestReserve, newSize.oldValue);
        }

        newSize.desc.set.compareAndSet(null, (ConcurrentHashMap<Integer, RWOperation>) localSet);


        return true;
    }



    // private CompactElement AcquireSizeElement() {
    //     // return the size in the compact size element

    //     return new CompactElement();
    // }

    private Boolean CompleteTransaction(Transaction desc, int lowerBounds) {

        boolean result = true;

        // reserve first 

        Set<Integer> indexes = desc.set.get().keySet();

        List<Integer> list = new ArrayList<>();
        for(Integer x : indexes) {
            if (x >= lowerBounds)
                list.add(x);
        }
        
        Collections.sort(list);

        // must perform read and writes from high to lo index 
        Collections.reverse(list);
        //System.out.println("the set has " + list.size());

        Iterator<Integer> it = list.iterator();        


        while(it.hasNext()) {

            int index = it.next();
            RWOperation rwop = desc.set.get().get(index);

            // Only read operations on an index
            if(rwop.lastWriteOp == null && rwop.readList.size() > 0) {
                
                // Then get value from shared memory
                int retval = v.ReadElement(index);

                for(int i = 0; i < rwop.readList.size(); i++) {
                    rwop.readList.get(i).returnValue = retval;
                }
            }

            else {
                CompactElement newElem = new CompactElement();
                newElem.desc = desc;
                newElem.newValue = rwop.lastWriteOp.value;
                result = v.UpdateElem(index, newElem);

                if(!result) {
                    System.out.println("update element failed\n");
                        return false;
                }
            }
        }

        desc.status.set(TxnStatus.committed);
        return true;

    }
}