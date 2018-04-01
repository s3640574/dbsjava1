import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;


public class dbquery {
	
	public static int count=0;
	public static String pageSize, searchQuery;
    public static byte[] REGISTER_NAME, BN_NAME, BN_STATUS, BN_REG_DT, BN_CANCEL_DT, BN_RENEW_DT, BN_STATE_NUM, BN_STATE_OF_REG, BN_ABN = null;
    public static DataOutputStream hFile = null;
	public static String fileName = "heap." + pageSize;
	private static BufferedReader reader;

	
    public static void main(String[] args) {
    	
        //String csvFile = "C:\\Users\\Victor Lee\\eclipse-dbs1\\DBHeapFile\\src\\BUSINESS_NAMES_201803.csv";
        //String csvFile = "C:\\Users\\Victor Lee\\eclipse-dbs1\\DBHeapFile\\src\\testline.txt";
    	String csvFile = "/home/ec2-user/javadbs1/BUSINESS_NAMES_201803.csv";
        BufferedReader br = null;

        String line = "";
        String cvsSplitBy = "\t";
        
        try {
        	searchQuery = args[0];
        	pageSize = args[1];
        	int pageSizeINT = Integer.parseInt(pageSize);       	
        	long startTime = System.nanoTime();       	

	        try {
	
	            br = new BufferedReader(new FileReader(csvFile));
	            hFile = new DataOutputStream(new FileOutputStream("heap." + pageSize));

	            int heapFileSize = 0;
	            int numOfPage = 1;
	            int numOfRecords = 0;

	
	            while ((line = br.readLine()) != null) {
	            	
	            	if (count > 0) {
	            			String[] fileLines = line.split(cvsSplitBy);
			                int lineLength = 0;           			
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
			                
			                for (int i=0;i<8;i++) {			                		                
				                try {
			                	heapFileSize = heapFileSize + fileLines[i].length();
			                	lineLength = lineLength + fileLines[i].length();
				                }catch (IndexOutOfBoundsException e) {
				                	
				                }
			                }
				               			    
				                
				                if (heapFileSize + lineLength < pageSizeINT) {
				                	writeFile();					                
					                numOfRecords++;			                
				                }
				                else {				                	
				                	hFile.close();
				                	scanFiles();		//scan heap.pagesize for query
				                	new File(fileName).delete();			                	
				                	hFile = new DataOutputStream(new FileOutputStream("heap." + pageSize));
				                	writeFile();					                
					                numOfRecords++;
					                numOfPage++;
				                	heapFileSize = lineLength;	//new heapfile
				                	
				                	
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
	
	    }//try args0
        catch (IndexOutOfBoundsException e){
        	System.out.println("Error! try --> $ java dbquery BN_NAME pagesize");
        }
    }//class
    
    private static void scanFiles() throws IOException {
    	reader = new BufferedReader(new FileReader("heap." + pageSize));
        String line = "";
        String cvsSplitBy = "\t";            
        
        while ((line = reader.readLine()) != null) {
	    	String[] token = line.split(cvsSplitBy);
	    	String tokenText0, tokenText1, tokenText2, tokenText3, tokenText4, tokenText5, tokenText6 = null, tokenText7 = null, tokenText8 = null;
	    	byte[] token0, token1, token2, token3, token4, token5, token6 = null, token7 = null, token8 = null;
	    	boolean boo6=false, boo7=false, boo8=false;
	    	
	        token0 = token[0].getBytes();
	        token1 = token[1].getBytes();
	        token2 = token[2].getBytes();
	        token3 = token[3].getBytes();
	        token4 = token[4].getBytes();
	        token5 = token[5].getBytes();        
	        try {
	        	token6 = token[6].getBytes();
	        }catch (IndexOutOfBoundsException e){	 
	        	boo6=true;
			}        
	        try {
	        	token7 = token[7].getBytes();
	        }catch (IndexOutOfBoundsException e) {   
	        	boo7=true;
	        }        
	        try {
	        	token8 = token[8].getBytes();
	        }catch (IndexOutOfBoundsException e) { 
	        	boo8=true;
	        }
	        
	        tokenText0 = new String(token0, "UTF-8");
	        tokenText1 = new String(token1, "UTF-8");
	        tokenText2 = new String(token2, "UTF-8");
	        tokenText3 = new String(token3, "UTF-8");
	        tokenText4 = new String(token4, "UTF-8");
	        tokenText5 = new String(token5, "UTF-8");	
	        if (boo6==false)
	        	tokenText6 = new String(token6, "UTF-8");
	        if (boo7==false)
	        	tokenText7 = new String(token7, "UTF-8");
	        if (boo8==false)
	        	tokenText8 = new String(token8, "UTF-8");
	        
	        if(tokenText1.toLowerCase().contains(searchQuery.toLowerCase()))
        	    System.out.println(tokenText0 + "\t" + tokenText1 + "\t" + tokenText2 + "\t" + tokenText3 + "\t" + tokenText4 + "\t" + tokenText5 + "\t" + tokenText6 + "\t" + tokenText7 + "\t" + tokenText8);
        }
		
	}



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


