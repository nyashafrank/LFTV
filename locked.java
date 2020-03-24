/* Group 5
 * COP 4520
 * Project Assignment 1 - Part 1
 * 3/11/2020
 */

import java.util.*; 
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;


public class locked {

    public static int totalOpTypes = 6;
    public static Random random = new Random();
    public static final int UNSET = Integer.MAX_VALUE;
    public static OperationType[] opTypeVals = OperationType.values();
    
    public static AtomicCompactVector v = new AtomicCompactVector();

    public static MRLOCK lockManager = new MRLOCK(100);


    public static void main(String[] args) {

        // Prepopulate compact vector
        

        // Maybe build all transactions beforehand and place in queue


        // Create threads
        Thread[] threads = new Thread[3];

        for(int i = 0; i < threads.length; i++)
        {
            Thread t = new Thread(new Perform(lockManager, random, opTypeVals));
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

        //System.out.println("\nExecution time = " + (endTime - startTime) + "ms\n");
       
    }
}




class Perform implements Runnable {

    public static Random r;
    public int totalOpTypes = 6;
    public final int UNSET = Integer.MAX_VALUE;
    public static  OperationType[] opTypeValues;
    public static MRLOCK lockManager;


    public Perform (MRLOCK manager, Random random, OperationType[] values) {
        r = random;
        opTypeValues = values;
        lockManager = manager;
    }

    public void run()
    {
        
        int txnCount = 0;

        while(txnCount < 1 /* change to true*/) {

            // Create Transaction
            Transaction t = BuildTransaction();
            System.out.println(t);

            // Preprocess transaction - turn into RWSet
            Preprocess(t);
            
            // Perform Transaction
            CompleteTransaction(t);

            txnCount++;
        }
    }

    public static void Preprocess(Transaction t) {

        RWOperation rwop;
        int largestReserve = 0;


        for (Operation op : t.operations) {   

            if(t.set.containsKey(op.index))
                rwop = t.set.get(op.index);
            
            else    
                rwop = new RWOperation();


            if(op.operationType == OperationType.read) {
                rwop.readList.add(op);
            }

            else if (op.operationType == OperationType.write) {
                rwop.lastWriteOp = op;
            }

            // Keep track of largest reserve call
            if(op.operationType == OperationType.reserve) {
               
                if(op.index > largestReserve)
                    largestReserve = op.index;
            }


            t.set.put(op.index, rwop);
        }

        // do something with the largestReserve value


    }


    public static Transaction BuildTransaction() {
        
        int operationCount = 0;
        Operation[] operations = new Operation[5];
        int value;

        while(operationCount < 5) {

            // get random operation type
            int opIndex = r.nextInt(6);
            OperationType opType = opTypeValues[opIndex];

            // get random value to write or push
            if(opType == OperationType.popBack)
                value = Integer.MAX_VALUE;
                
            else
                value = r.nextInt(100);

            // choose random index to perform operation on in vector
            // Change the random bounds
            int vectorIndex = r.nextInt(100);

            Operation operation = new Operation(opType, value, vectorIndex);
            operations[operationCount] = operation;

            operationCount++;
        }
        
        Transaction t = new Transaction(operations);

        return t;
    }


    public static void CompleteTransaction(Transaction desc) {

        ExecuteOps(desc);
        desc.status.set(TxnStatus.committed);

    }


    private static void ExecuteOps(Transaction desc) {
       
        BitSet request = new BitSet();

        Set<Integer> indexes = desc.set.keySet();

        Iterator<Integer> it = indexes.iterator();
        
        // Setup the resource request
        while(it.hasNext()) {

            // set each bit for every index we want to access
            request.set(it.next());
        }

        Integer position = lockManager.lock(request);

        // Complete operations here
       //   UpdateElement();

        System.out.println("\nThread " +Thread.currentThread().getName() + " has completed\n" + desc);

        lockManager.unlock(position);
    }


/*
    public static Boolean UpdateElement(Transaction desc, int index, RWOperation rwop) {


        // out of bounds access
        if(index > v.array.length) {
            desc.status.set(TxnStatus.aborted);
            return false;
        }

        CompactElement newElem = new CompactElement();
        CompactElement oldElem = v.array[index];

   
        // should probably get rid of these atomics for the sequential version
        if(oldElem.desc.status.get() == TxnStatus.committed && oldElem.desc.set != null) {

            if(oldElem.desc.set.containsKey(index)) {
                Operation oldElemLastWrite = oldElem.desc.set.get(index).lastWriteOp;
                if( oldElemLastWrite != null) {
                    newElem.oldValue = oldElem.newValue;
                }
                else newElem.oldValue = oldElem.oldValue;
            }
                
        }
        else newElem.oldValue = oldElem.oldValue;


        // accessing out of bounds
        if(rwop.checkBounds && newElem.oldValue == UNSET) {
            desc.status.set(TxnStatus.aborted);
            return false;
        }

        // perform the write operation or maybe not
        if(rwop.lastWriteOp != null) {
            newElem.newValue = rwop.lastWriteOp.value;
        }
        else {
            newElem.oldValue = v.array[index].oldValue;
        }
            
        
        
        newElem.desc = desc;

        v.array[index] = newElem;

        for(int i = 0; i < rwop.readList.size(); i++) {
            Operation op = rwop.readList.get(i);
            op.returnValue = newElem.oldValue;
        }


        return true;
    }
    */
}
