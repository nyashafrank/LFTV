/* Group 5
 * COP 4520
 * Project Assignment 1 - Part 1
 * 3/11/2020
 */

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;


public class locked {

    public static int totalOpTypes = 6;
    public static final int UNSET = Integer.MAX_VALUE;
    public static OperationType[] opTypeVals = OperationType.values();
    
    public static CompactLFTV v = new CompactLFTV();

    public static MRLOCK lockManager = new MRLOCK(100);


    public static void main(String[] args) {

        // Prepopulate compact vector
        for(int i = 0; i <  8; i++)
            v.Populate();

        // Maybe build all TransactionRegs beforehand and place in queue


        // Create threads
        Thread[] threads = new Thread[7];

        for(int i = 0; i < threads.length; i++)
        {
            TransactionReg[] transactions = BuildTransactions();
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
    
        
    }


    // Creates an array of transactions for a thread to pull from
    public static TransactionReg[] BuildTransactions() {

        TransactionReg[] transactions = new TransactionReg[1];        // ******** CHANGE THIS NUMBER LATER *********

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
                int vectorIndex = ThreadLocalRandom.current().nextInt(50);         // ******** CHANGE THE BOUND *********
    
                Operation operation = new Operation(opType, value, vectorIndex);
                operations[operationCount] = operation;
    
                operationCount++;
            }

            TransactionReg t = new TransactionReg(operations);

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


    public MRLOCK lockManager;
    public CompactLFTV v;
    TransactionReg[] transactions;



    public Perform (MRLOCK manager, CompactLFTV vector, TransactionReg[] t) {
        lockManager = manager;
        v = vector;
        transactions = t;
    }

    public void run() 
    {
        for(int i = 0; i < transactions.length; i++) {

            TransactionReg t = transactions[i];
            Preprocess(t);
            CompleteTransaction(t);

           // System.out.println("Thread " + Thread.currentThread().getName() + "\n" + t);

           // v.PrintVector();
        }
    }

    public static void Preprocess(TransactionReg t) {

        RWOperation rwop = null;
        int largestReserve = 0;
       // int possibleSize = v.size.get();

        for (int i = 0; i < t.operations.length; i++) {   
            Operation op = t.operations[i];

            if(t.set.containsKey(op.index)) 
                rwop = t.set.get(op.index);
            else    
                rwop = new RWOperation();

            if(op.operationType == OperationType.read) {
                
                // Add read to the list so we can store the elements oldval in its return in updateElement
                if(rwop.lastWriteOp == null){
                //   /  rwop.checkBounds = true;
                //     rwop.readList.add(op);
                //     t.set.put(op.index, rwop);
                }
                else {
                    op.returnValue = rwop.lastWriteOp.value;
                }
            }

            else if (op.operationType == OperationType.write) {
                rwop.checkBounds = true;
                rwop.lastWriteOp = op;
                t.set.put(op.index, rwop);
            }

            else if (op.operationType == OperationType.size) {
       //         op.returnValue = possibleSize;
            }

            else {

            }
/*
            else if (op.operationType == OperationType.popBack) {
                possibleSize--;
                if(t.set.containsKey(possibleSize)) 
                    rwop = t.set.get(possibleSize);
                else    
                    rwop = new RWOperation();
                op.index = possibleSize;
                rwop.checkBounds = false;
                rwop.readList.add(op);
                rwop.lastWriteOp = op;
                t.set.put(op.index, rwop);
            }

            else if (op.operationType == OperationType.pushBack) {
                if(t.set.containsKey(possibleSize)) 
                    rwop = t.set.get(possibleSize);
                 else    
                    rwop = new RWOperation();
                rwop.checkBounds = false;
                rwop.lastWriteOp = op;
                t.set.put(possibleSize, rwop);
                possibleSize++;
                largestReserve = Math.max(largestReserve, possibleSize);
            }


            // Keep track of largest reserve call
            if(op.operationType == OperationType.reserve) {
                if(op.index > largestReserve)
                    largestReserve = op.index;
            }
        */
        }

        // do something with the largestReserve value
        // if(largestReserve > 0 && largestReserve > v.array.length)
        //         v.Reserve(largestReserve);
    
      
              
    }

    private void CompleteTransaction(TransactionReg desc) {
       
        BitSet request = new BitSet();

        Set<Integer> indexes = desc.set.keySet();

        Iterator<Integer> it = indexes.iterator();
        
        // Setup the resource request
        while(it.hasNext()) {
           
            int index = it.next();
            
            RWOperation rwop = desc.set.get(index);
            if(rwop.lastWriteOp == null && rwop.readList.size() > 0) {
                
                // Then get value from shared memory
                int retval = v.ReadElement(index);

                for(int i = 0; i < rwop.readList.size(); i++) {
                    rwop.readList.get(i).returnValue = retval;
                }
            }

            // set each bit for every index we want to modify
            else
                request.set(index);
        }

        if(request.cardinality() == 0)
            return;

        BitSet t = (BitSet)request.clone();
      //  lockManager.printRequests();
      //  System.out.println(Thread.currentThread().getName() + " request " + request + "\n");



        // ACQUIRE LOCK
        Integer position = lockManager.lock(request);

        System.out.println(Thread.currentThread().getName() + " request " + request + " lock acquired\n");
        

        // Complete operations here
    

       // System.out.println("\nThread " +Thread.currentThread().getName() + " has completed\n" + desc);

       // RELEASE LOCK
        lockManager.unlock(position);

      /*  if(request.cardinality() == 64)
            System.out.println(Thread.currentThread().getName() + " request " +t + " unlocked\n");
        
        */
    }
}
