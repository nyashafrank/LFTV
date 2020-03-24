/* Group 5
 * COP 4520
 * Project Assignment 1 - Part 1
 * 3/11/2020
 */

import java.util.*; 


public class sequential {

    public static final int UNSET = Integer.MAX_VALUE;
    //public static CompactElement[] CompactVector = new CompactElement[10];
    public static OperationType[] opTypeValues = OperationType.values();
    public static int totalOpTypes = 6;
    public static CompactVector v = new CompactVector();

    public static Random random = new Random();

    public static void main(String[] args) {
                
        Populate();

        // for(int i = 0; i < v.array.length; i++) {
        //     System.out.println(v.array[i]);
        // }

        int txnCount = 0;

        while(txnCount < 2) {

            // Create Transaction
            Transaction t = BuildTransaction();

            // Preprocess transaction - turn into RWSet
            Preprocess(t);

            // Perform Transaction
            Boolean result = CompleteTransaction(t);

            // if(result)
            //     System.out.println("Completed\n" + t);
            // else
            //     System.out.println("Aborted\n" + t);


            txnCount++;
        }

        //   System.out.println();

        //  //print out final state of the vector
        //   for(int i = 0; i < v.array.length; i++) {
        //       System.out.println(v.array[i]);
        //   }
     }

    // Preprocessing 
    private static int Preprocess(Transaction t) {

        RWOperation rwop = null;
        int largestReserve = 0;
        int possibleSize = v.size;

        for (int i = 0; i < t.operations.length; i++) {   
            Operation op = t.operations[i];

            if(t.set.containsKey(op.index)) 
                rwop = t.set.get(op.index);
            
            else    
                rwop = new RWOperation();

            if(op.operationType == OperationType.read) {
                rwop.checkBounds = true;
                rwop.readList.add(op);
                t.set.put(op.index, rwop);
            }

            else if (op.operationType == OperationType.write) {
                rwop.checkBounds = true;
                rwop.lastWriteOp = op;
                t.set.put(op.index, rwop);
            }

            else if (op.operationType == OperationType.size) {
                op.index = possibleSize;
            }

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
        }

        // do something with the largestReserve value
        if(largestReserve > 0 && largestReserve > v.array.length)
                v.Reserve(largestReserve);

        return largestReserve;

    }


    // Populates the vector by using pushback 
    private static void Populate() {

        int count = 0;

        // pushback values into vector
        while(count < 9) {
            v.Populate(random.nextInt(100));
            count++;
        }
    } 
 

    private static Transaction BuildTransaction() {
        
        int operationCount = 0;
        Operation[] operations = new Operation[5];
        int value;

        while(operationCount < 5) {

            // get random operation type
            int opIndex = random.nextInt(totalOpTypes);
            OperationType opType = opTypeValues[opIndex];

            // get random value to write or push
            if(opType == OperationType.popBack)
                value = Integer.MAX_VALUE;
                
            else
                value = random.nextInt(100);

            // choose random index to perform operation on in vector
            // Change the random bounds
            int vectorIndex = random.nextInt(10);

            Operation operation = new Operation(opType, value, vectorIndex);
            operations[operationCount] = operation;

            operationCount++;
        }
        
        Transaction t = new Transaction(operations);
        t.size = 5;

        return t;
    }


    private static Boolean CompleteTransaction(Transaction desc) {

        boolean ret = true;

            // reserve first 

            Set<Integer> indexes = desc.set.keySet();

            List<Integer> list = new ArrayList<>();
            for(Integer x : indexes) {
                list.add(x);
            }
            
            Collections.sort(list);

            // must perform read and writes from high to lo index 
            Collections.reverse(list);

            
            Iterator<Integer> it = list.iterator();        

            while(it.hasNext()) {

                int index = it.next();

                RWOperation rwop = desc.set.get(index);

                Boolean result = UpdateElement(desc, index, rwop);


                if(!result) {
                    return false;
                }
            }

            desc.status.set(TxnStatus.committed);
            return true;
    }

    private static Boolean UpdateElement(Transaction desc, int index, RWOperation rwop) {


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
}
