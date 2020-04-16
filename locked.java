/* Group 5
 * COP 4520
 * Project Assignment 1 - Part 1
 * 3/11/2020
 */

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;


public class locked {

    public static int size = 0;
    public static int completedTransactions = 0;
    public static Object lock = new Object();
    public static Object completedTransactionslock = new Object();
    public static int totalOpTypes = 6;
    public static final int UNSET = Integer.MAX_VALUE;
    public static OperationType[] opTypeVals = OperationType.values();
    
    public static CompactLFTV v = new CompactLFTV();

   // public static MRLOCK lockManager = new MRLOCK(100);
    public static MRSimpleLock lockManager = new MRSimpleLock();

    public static void main(String[] args) {

        // Prepopulate compact vector
       // v.Reserve();
        //for(int i = 0; i <  1024; i++)
            v.Populate(1024);

        size = v.size.get().newValue;
        //v.Reserve(8*2);

            System.out.println("\t------ Initial State ------\n");
          //  v.PrintVector();

           System.out.println("vector size " + v.size.get().newValue);


        // Create threads
        Thread[] threads = new Thread[8];

        for(int i = 0; i < threads.length; i++)
        {
            Transaction[] transactions = BuildTransactions();
            Thread t = new Thread(new Perform(lockManager, v, transactions));
            t.setName(String.valueOf(i+1));
            threads[i] = t;
        }

        // Start timer
        long startTime = System.currentTimeMillis();

        // Start threads
        for(int i = 0; i < threads.length; i++)
            threads[i].start();

        try {

            // Join threads
            for (Thread thread : threads) 
                thread.join();

        } catch(InterruptedException e){
            System.out.println("Threads interrupted");
        }

        long endTime = System.currentTimeMillis();

        System.out.println("\nExecution time = " + (endTime - startTime) + "ms\n");

        
        CompactElement currentSize = v.size.get();
        CompactElement newSize = new CompactElement(currentSize);

        newSize.newValue = size;
        v.size.compareAndSet(currentSize, newSize);

        System.out.println("\n\n\t------ Final State ------\n");
       // v.PrintVector();

      //  System.out.println("\n" + v.buckets.get(0).get(20));


        System.out.println("final size of vector is " + size + "\nNumber of completed transactions is " + completedTransactions);
        
    }


    // Creates an array of transactions for a thread to pull from
    public static Transaction[] BuildTransactions() {

        Transaction[] transactions = new Transaction[5000];        // ******** CHANGE THIS NUMBER LATER *********

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
                int vectorIndex = ThreadLocalRandom.current().nextInt(900);         // ******** CHANGE THE BOUND *********
    
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
        if(ratio < 0.33)
            return opTypeVals[0];
    
        // Write operation - 20%
        else if (ratio >= 0.33 && ratio < 0.49)
            return opTypeVals[1];
        
        // Pushback - 20%
        else if (ratio >= 0.49 && ratio < 0.82)
            return opTypeVals[2];

        // Popback - 20%
        else if (ratio >= 0.82 && ratio < 0.98)
            return opTypeVals[3];

        // Size - 10%
        else if (ratio >= 0.98 && ratio < 0.99)
            return opTypeVals[4];

        // Reserve - 10%
        else
            return opTypeVals[5];
    }

}




class Perform implements Runnable {


    //public MRLOCK lockManager;
    public MRSimpleLock lockManager;
    public static CompactLFTV v;
    Transaction[] transactions;



    public Perform (MRSimpleLock manager, CompactLFTV vector, Transaction[] t) {
        lockManager = manager;
        v = vector;
        transactions = t;
    }

    public void run() 
    {
        for(int i = 0; i < transactions.length; i++) {

            Transaction t = transactions[i];
            int preprocessingSize = Preprocess(t);


        // System.out.println("completing transaction");
            CompleteTransaction(t);

            // if transaction committed, then update the vector size
            if(t.status.get() == TxnStatus.committed) {
               // System.out.println("inside trying to chanfe size");
                synchronized(locked.lock){
                    locked.size += preprocessingSize;
                    locked.completedTransactions++;
                    //System.out.println("new size is " + locked.size);

                }
            }
        }

        //System.out.println("\n" + v.buckets.get(0).get(20));
    }

    public static int Preprocess(Transaction t) {

        ConcurrentHashMap<Integer,RWOperation> localSet = new ConcurrentHashMap<Integer, RWOperation>();
        
        RWOperation rwop = null;
        int largestReserve = 0;
        boolean sizeAcquired = false;
        
        int value = 0;
        int possibleSize = 0;

        for (int i = 0; i < t.operations.length; i++) {   
            Operation op = t.operations[i];

           // System.out.println("placing in index " + op.index);
            if(localSet.containsKey(op.index)) 
                rwop = localSet.get(op.index);
            else    
                rwop = new RWOperation();


            // READ OPERATION
            if(op.operationType == OperationType.read) {
                
                if(!sizeAcquired) {
                    synchronized(locked.lock){
                        possibleSize = locked.size;
                        sizeAcquired = true;
                    }
                }
                
                if (op.index >= possibleSize) {
                    t.status.set(TxnStatus.aborted);
                    //System.out.println("somethings up");
                    return Integer.MIN_VALUE;
                }
                

                // Add read to the list so we can store the elements oldval in its return in updateElement
                if(rwop.lastWriteOp == null){
             
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

                if(!sizeAcquired) {
                    synchronized(locked.lock){
                        possibleSize = locked.size;
                        sizeAcquired = true;
                    }
                }

                if (op.index >= possibleSize) {
                    t.status.set(TxnStatus.aborted);
                   // System.out.println("somethings up2");
                    return Integer.MIN_VALUE;
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

                if(!sizeAcquired) {
                    synchronized(locked.lock){
                        possibleSize = locked.size;
                        sizeAcquired = true;
                    }
                }
                
                op.returnValue = possibleSize;
            }


            // POPBACK OPERATION
            else if (op.operationType == OperationType.popBack) {

                if(!sizeAcquired){
                    synchronized(locked.lock){
                        possibleSize = locked.size;
                        sizeAcquired = true;
                    }
                }

                value--;
                possibleSize--;

                if(localSet.containsKey(possibleSize)) 
                    rwop = localSet.get(possibleSize);
                else    
                    rwop = new RWOperation();

                op.index = possibleSize;
                rwop.readList.add(op);
                rwop.lastWriteOp = op;
                localSet.put(possibleSize, rwop);
            }


            // PUSHBACK OPERATION
            else if (op.operationType == OperationType.pushBack) {

                if(!sizeAcquired){
                    synchronized(locked.lock){
                        possibleSize = locked.size;
                        sizeAcquired = true;
                    }
                }

                if(localSet.containsKey(possibleSize)) {
                    //System.out.println("Found another pushback? on " + possibleSize + " for value " + op.value);
                    rwop = localSet.get(possibleSize);
                }
                    
                 else    {
                   // System.out.println("no other pushback on " + possibleSize + " for value " + op.value);
                   rwop = new RWOperation();
                }
                   
                    

                rwop.lastWriteOp = op;
                localSet.put(possibleSize, rwop);
                possibleSize++;
                value++;
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
            v.Reserve(largestReserve);
        }          
        
        t.set.set(localSet);

        return value;
    }



    private void CompleteTransaction(Transaction desc) {
       
        BitSet request = new BitSet(4096);

       // System.out.println("desc.set is " + desc.set.get());
       try{
           desc.set.get().keySet();
       }
       catch(Exception e){
          // System.out.println("okay the error is " + e);
           desc.status.set(TxnStatus.aborted);
           //System.out.println("somethings up3");
           return;
       }
        // if(desc.set.get(){
            
        //     System.out.println("for some reason its empty");
        //     desc.status.s
        // }

        Set<Integer> indexes = desc.set.get().keySet();

        // Object[] requestInd = indexes.toArray();
        // System.out.println("The indexes are ");
        // for(int i = 0; i < requestInd.length; i++){
        //     System.out.print(requestInd[i] + " ");
        // }

        // System.out.println("done");

        ArrayList<Integer> requestIndexes = new ArrayList<>(); 

        Iterator<Integer> it = indexes.iterator();
        
        // Setup the resource request
        while(it.hasNext()) {
           
            int index = it.next();
            //System.out.println("index is "+index);
            
            RWOperation rwop = desc.set.get().get(index);
            if(rwop.lastWriteOp == null && rwop.readList.size() > 0) {
                
                // Then get value from shared memory
                int retval = v.ReadElement(index);

                for(int i = 0; i < rwop.readList.size(); i++) {
                    rwop.readList.get(i).returnValue = retval;
                }
            }

            // set each bit for every index we want to modify
            else{
                requestIndexes.add(index);
                request.set(index);
            }
                
        }

        if(request.cardinality() == 0){
            desc.status.set(TxnStatus.committed);
            return;
        }
            

       // BitSet t = (BitSet)request.clone();



        // ACQUIRE LOCK
        lockManager.lock(request);

        //System.out.println(Thread.currentThread().getName() + " request " + request + " lock acquired\n");
        

        // Complete operations here
        for(int i = 0; i < requestIndexes.size(); i++){
            int index = requestIndexes.get(i);

            RWOperation rwop = desc.set.get().get(index);

            CompactElement newElem = new CompactElement();
                newElem.desc = desc;
                newElem.newValue = rwop.lastWriteOp.value;
                boolean result = v.UpdateElemNoHelping(index, newElem);

                if(!result) {
                  //  System.out.println("update element failed\n");
                   // System.out.println("somethings up4");
                    desc.status.set(TxnStatus.aborted);
                    newElem.desc.status.set(TxnStatus.aborted);
                    lockManager.unlock(request);
                    return;
                }

                desc.status.set(TxnStatus.committed);
        }

    

       // System.out.println("\nThread " +Thread.currentThread().getName() + " has completed\n" + desc);

       // RELEASE LOCK
        lockManager.unlock(request);
    }
}
