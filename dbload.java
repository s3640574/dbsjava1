import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;


public class dbload {
	

	public static int count=0;
	public static String pageSize, searchQuery;
    public static byte[] REGISTER_NAME, BN_NAME, BN_STATUS, BN_REG_DT, BN_CANCEL_DT, BN_RENEW_DT, BN_STATE_NUM, BN_STATE_OF_REG, BN_ABN = null;
    public static DataOutputStream hFile = null;
	
    public static void main(String[] args) {
    	
        //String csvFile = "C:\\Users\\Victor Lee\\eclipse-dbs1\\DBHeapFile\\src\\BUSINESS_NAMES_201803.csv";
        //String csvFile = "C:\\Users\\Victor Lee\\eclipse-dbs1\\DBHeapFile\\src\\testline.txt";
    	String csvFile = "/home/ec2-user/javadbs1/BUSINESS_NAMES_201803.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = "\t";
        
        if(args[0].compareTo("-p") == 0) {
        	pageSize = args[1];
        	int pageSizeINT = Integer.parseInt(pageSize);
        	long startTime = System.nanoTime();
	        try {
	
	            br = new BufferedReader(new FileReader(csvFile));
	            hFile = new DataOutputStream(new FileOutputStream("heap." + pageSize));

	            int heapFileSize = 0;
	            int numOfPage = 1;
	            int numOfRecords = 0;
	            String fileName = "heap." + pageSize;
	
	            while ((line = br.readLine()) != null) {
	            	
	            	if (count > 0) {
	            			String[] fileLines = line.split(cvsSplitBy);
			                            			
			                REGISTER_NAME = fileLines[0].getBytes();
			                BN_NAME = fileLines[1].getBytes();
			                BN_STATUS = fileLines[2].getBytes();
			                BN_REG_DT = fileLines[3].getBytes();
			                BN_CANCEL_DT = fileLines[4].getBytes();
			                BN_RENEW_DT = fileLines[5].getBytes();
			                
			                try {
			                BN_STATE_NUM = fileLines[6].getBytes();
			                }catch (IndexOutOfBoundsException e){	
			                	
							}
			                
			                try {
			                BN_STATE_OF_REG = fileLines[7].getBytes();
			                }catch (IndexOutOfBoundsException e) {
			                	
			                }
			                
			                try {
			                BN_ABN = fileLines[8].getBytes();
			                }catch (IndexOutOfBoundsException e) {
			                	
			                }			                			                
			                //System.out.println(fileLines[0] + "\t" + fileLines[1] + "\t" + fileLines[2] + "\t" + fileLines[3] + "\t" + fileLines[4] + "\t" + fileLines[5] + "\t" + fileLines[6] + "\t" + fileLines[7] + "\t" + fileLines[8]);
			                			                
			                for (int i=0;i<8;i++) {			                		                
				                try {
			                	heapFileSize = heapFileSize + fileLines[i].length();
				                }catch (IndexOutOfBoundsException e) {
				                	
				                }
			                }
				               			    				                
				                if (heapFileSize < pageSizeINT) {
				                	writeFile();					                
					                numOfRecords++;			                
				                }
				                else {
				                	hFile.close();
				                	new File(fileName).delete();
				                	hFile = new DataOutputStream(new FileOutputStream("heap." + pageSize));
				                	writeFile();
					                
					                numOfRecords++;
					                numOfPage++;
				                	heapFileSize = 0;	//new heapfile
				                					                	
				                }	
	            	}//end of if 
	            	else	
	            	count = 1;
	            }//while
	         long stopTime = System.nanoTime();
	         long timeUsed = (stopTime - startTime) / 1000000;
	         System.out.println("Time used = " + timeUsed + " milliseconds");   
	         System.out.println("Records loaded = " + numOfRecords);
	         System.out.println("Page used = " + numOfPage);
	        }//try 
        
        
        
        
	        catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (br != null) {
	                try {
	                    br.close();
	                    hFile.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	
	    }//if args[0]
        else if(args[0] == null || args[1] == null || args[0] != "-p")
        	System.out.println("Error! try --> $ java dbload -p pagesize");
        else
        	System.out.println("Command: $ java dbload -p pagesize");


    }//class
    
    private static void writeFile() throws IOException {

        hFile.write(REGISTER_NAME); hFile.write(0x09);		//0x09 = \t
        hFile.write(BN_NAME); hFile.write(0x09);
        hFile.write(BN_STATUS); hFile.write(0x09);
        hFile.write(BN_REG_DT); hFile.write(0x09);
        hFile.write(BN_CANCEL_DT); hFile.write(0x09);
        hFile.write(BN_RENEW_DT); hFile.write(0x09);
        
        if (BN_STATE_NUM != null)
        hFile.write(BN_STATE_NUM); hFile.write(0x09);

        if (BN_STATE_OF_REG != null)
        hFile.write(BN_STATE_OF_REG); hFile.write(0x09);
    
        if (BN_ABN != null)
        hFile.write(BN_ABN); hFile.write(0x0A);		//0x0A = \n
	}

}//dbload
