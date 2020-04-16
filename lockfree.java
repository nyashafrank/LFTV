import java.util.concurrent.ThreadLocalRandom;

public class lockfree {
    // CHANGE THIS TO VARY PREPOPULATION AND RANGE OF OPERATIONS
    private static final int INITIAL_SIZE = 1024;
    private static final int TRANSACTIONS_PER_THREAD = 5000;

    public static int totalOpTypes = 6;
    public static final int UNSET = Integer.MAX_VALUE;
    public static OperationType[] opTypeVals = OperationType.values();

    public static CompactLFTV lftv = new CompactLFTV();

    public static void main(String[] args) {
        lockfree lockfree = new lockfree();
        int[] threadCount = new int[]{1, 8, 16, 24, 32};

        for (int trial = 1; trial <= 3; trial++) {

            for (int j = 0; j < threadCount.length; j++) {
                lftv = new CompactLFTV();
                lftv.Populate(INITIAL_SIZE);

                Thread[] threads = new Thread[threadCount[j]]; 

                for(int i = 0; i < threads.length; i++) {
                    Transaction[] transactions = BuildTransactions(trial);
                    Thread t = new Thread(lockfree.new Perform(transactions, lftv));  

                    t.setName(String.valueOf(i+1));
                    threads[i] = t;
                }

                long startTime = System.currentTimeMillis();

                for(int i = 0; i < threads.length; i++)
                    threads[i].start();

                try {
                    for (Thread thread : threads) 
                        thread.join();
                } catch(InterruptedException e){
                    System.out.println("Threads interrupted");
                }

                long endTime = System.currentTimeMillis();
                
                System.out.println("\n Trial: " + trial + ", Thread Count: " + threadCount[j] + ", Execution time = " + (endTime - startTime) + "ms\n ");
            }
        }
    }



    // Creates an array of transactions for a thread to pull from
    public static Transaction[] BuildTransactions(int trial) {

        Transaction[] transactions = new Transaction[TRANSACTIONS_PER_THREAD]; 

        // Build each transaction and insert it into transactions array
        for(int x = 0; x < transactions.length; x++) {

            int value;
            int operationCount = 0;
            Operation[] operations = new Operation[5];
    
            // Insert 5 random operations per transaction
            while(operationCount < 5) {

                // get random operation type
                double ratio = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
                OperationType opType = GetOperationType(ratio, trial);
    
                // Popback's value field should always be max integer
                if(opType == OperationType.popBack)
                    value = Integer.MAX_VALUE;
                    
                // get random value to write or push
                else
                    value = ThreadLocalRandom.current().nextInt(100);
    
                // choose random index to perform operation on in vector
                int vectorIndex = ThreadLocalRandom.current().nextInt(INITIAL_SIZE);
    
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
    public static OperationType GetOperationType(double ratio, int trial) {
        if (trial == 1) {
            // Read                                      
            if(ratio < 0.2)
                return opTypeVals[0];
        
            // Write
            else if (ratio >= 0.2 && ratio < 0.4)
                return opTypeVals[1];
            
            // Pushback
            else if (ratio >= 0.4 && ratio < 0.6)
                return opTypeVals[2];

            // Popback
            else if (ratio >= 0.6 && ratio < 0.8)
                return opTypeVals[3];

            // Size
            else if (ratio >= 0.8 && ratio < 0.9)
                return opTypeVals[4];

            // Reserve
            else
                return opTypeVals[5];
        } else if (trial == 2) {
            // Read                                      
            if(ratio < 0.33)
            return opTypeVals[0];
        
            // Write
            else if (ratio >= 0.33 && ratio < 0.49)
                return opTypeVals[1];
            
            // Pushback
            else if (ratio >= 0.49 && ratio < 0.82)
                return opTypeVals[2];

            // Popback
            else if (ratio >= 0.82 && ratio < 0.98)
                return opTypeVals[3];

            // Size
            else if (ratio >= 0.98 && ratio < 0.99)
                return opTypeVals[4];

            // Reserve
            else
                return opTypeVals[5];
        } else if (trial == 3) {
            // Read                                      
            if(ratio < 0.4)
            return opTypeVals[0];
        
            // Write
            else if (ratio >= 0.4 && ratio < 0.49)
                return opTypeVals[1];
            
            // Pushback
            else if (ratio >= 0.49 && ratio < 0.89)
                return opTypeVals[2];

            // Popback
            else if (ratio >= 0.89 && ratio < 0.98)
                return opTypeVals[3];

            // Size
            else if (ratio >= 0.98 && ratio < 0.99)
                return opTypeVals[4];

            // Reserve
            else
                return opTypeVals[5];
        }

        return null;
    }



    class Perform implements Runnable {

        public Transaction[] transactions;
        public CompactLFTV v;
        public CompactElement possibleSize;
        public CompactElement oldSize;
        public int vectorSize;
        CompactElement newSize;

        public Perform (Transaction[] t, CompactLFTV lftv) {     
            this.transactions = t;
            this.v = lftv;
        }

        public void run() {
            for(int i = 0; i < transactions.length; i++) {
                Transaction t = transactions[i];

                boolean success = v.Preprocess(t);
                if(success)
                    v.CompleteTransaction(t, 0);
                else 
                    t.status.set(TxnStatus.aborted);
            }
        }
    }

}
