import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class dbload{
	static int pageSize;
	//static String dataFile = "C:\\Users\\Administrator\\eclipse-workspace\\db2\\src\\BUSINESS_NAMES_201803.csv";
	static String dataFile = "/home/ec2-user/db2pt3/BUSINESS_NAMES_201803.csv";


	public static void main (String[] args){

			if(args[0].equals("-p")){
				pageSize = Integer.parseInt(args[1]);			
			}
			else {
				System.out.println("Error!.");
				System.exit(0);
			}
		
		BufferedReader br = null;
		DataOutputStream os = null;
		String csvDelimiter = "\t";
		String line;
		String BN_NAME, BN_STATUS, BN_REG_DT, BN_CANCEL_DT, BN_RENEW_DT, BN_STATE_NUM, BN_STATE_OF_REG, BN_ABN;

		int BN_NAMEsize = 200;
		int BN_STATUSsize = 1;
		int BN_REG_DTsize = 10;
		int BN_CANCEL_DTsize = 10;
		int BN_RENEW_DTsize = 10;
		int BN_STATE_NUMsize = 10;
		int BN_STATE_OF_REGsize = 2;
		int BN_ABNsize = 20;
		int recordSize = BN_NAMEsize + BN_STATUSsize + BN_REG_DTsize + BN_CANCEL_DTsize + BN_RENEW_DTsize + BN_STATE_NUMsize + BN_STATE_OF_REGsize + BN_ABNsize;

		if(pageSize<recordSize){
			System.out.println(
				"Page size should be larger or equal to " + recordSize + 
				"\nError.");
			System.exit(0);
		}
		int recordPerPage = pageSize/recordSize;
		int remainderPage = pageSize%recordSize;		
		int numOfRecord = 0;
		int numOfPage = 1;
		long startTime = System.nanoTime();

		try{
			br = new BufferedReader(new FileReader(dataFile));
			os = new DataOutputStream(new FileOutputStream("heap." + pageSize));
			br.readLine();

			int currRec = 0;
			while((line = br.readLine()) != null){
				//separate the line
				String[] split = line.split(csvDelimiter);
				BN_NAME = split[1];
				BN_STATUS = split[2];
				BN_REG_DT = split[3];
				BN_CANCEL_DT = split[4];
				BN_RENEW_DT = split[5];
				try{
					BN_STATE_NUM = split[6];
				} catch (IndexOutOfBoundsException e){
					BN_STATE_NUM = "";
				}
				try{
					BN_STATE_OF_REG = split[7];
				} catch (IndexOutOfBoundsException e){
					BN_STATE_OF_REG = "";
				}
				try{
					BN_ABN = split[8];
					BN_ABN = BN_ABN.replace("\n", "");
				} catch (IndexOutOfBoundsException e){
					BN_ABN = "";
				}


				byte[] byteName = BN_NAME.getBytes();
				byte[] namePad = Arrays.copyOf(byteName, BN_NAMEsize);
				
				boolean bolStatus = BN_STATUS.contentEquals("Registered");

				byte[] byteRegDate = BN_REG_DT.getBytes();
				byte[] regDtPad = Arrays.copyOf(byteRegDate, BN_REG_DTsize);

				byte[] byteCancelDate = BN_CANCEL_DT.getBytes();
				byte[] cancelDtPad = Arrays.copyOf(byteCancelDate, BN_CANCEL_DTsize);

				byte[] byteRenewDate = BN_RENEW_DT.getBytes();
				byte[] renewDtPad = Arrays.copyOf(byteRenewDate, BN_RENEW_DTsize);
				
				byte[] byteStateNum = BN_STATE_NUM.getBytes();
				byte[] stateNumPad = Arrays.copyOf(byteStateNum, BN_STATE_NUMsize);
				
				short shtState = mapState.get(BN_STATE_OF_REG);

				byte[] byteABN = BN_ABN.getBytes();
				byte[] abnPad = Arrays.copyOf(byteABN, BN_ABNsize);

				os.write(namePad);
				os.writeBoolean(bolStatus);
				os.write(regDtPad);
				os.write(cancelDtPad);
				os.write(renewDtPad);
				os.write(stateNumPad);
				os.writeShort(shtState);
				os.write(abnPad);

				currRec++;
				numOfRecord++;
				if(currRec == recordPerPage){
					currRec = 0;
					numOfPage++;
					for(int i=0; i<remainderPage; i++){
						os.write(0);
					}
				}
			}
			for(int i=0; i<(recordPerPage-currRec)*recordSize+remainderPage; i++){
				os.write(0);
			}
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (br != null){
				try{
					br.close();
				} catch (IOException e){
					e.printStackTrace();
				}
			}
			if (os != null){
				try{
					os.close();
				} catch (IOException e){
					e.printStackTrace();
				}
			}
		}
        long stopTime = System.nanoTime();
        long timeUsed = (stopTime - startTime) / 1000000;
		System.out.println("Time used = " + timeUsed + " milliseconds");
        System.out.println("Records loaded = " + numOfRecord);
        System.out.println("Page used = " + numOfPage);

	}


	private static final Map<String, Short> mapState = new HashMap<String, Short>();
	static{
		mapState.put("", (short)0);
		mapState.put("NSW", (short)1);
		mapState.put("ACT", (short)2);
		mapState.put("VIC", (short)3);
		mapState.put("QLD", (short)4);
		mapState.put("SA", (short)5);
		mapState.put("WA", (short)6);
		mapState.put("TAS", (short)7);
		mapState.put("NT", (short)8);
	}
}