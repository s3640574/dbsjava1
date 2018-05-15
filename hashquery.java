import java.io.*;
import java.util.*;


public class hashquery{

	private static final int INTBYTES = 4;
	private static final int BN_NAMEsize = 200;
	private static final int BN_STATUSsize = 1;
	private static final int BN_REG_DTsize = 10;
	private static final int BN_CANCEL_DTsize = 10;
	private static final int BN_RENEW_DTsize = 10;
	private static final int BN_STATE_NUMsize = 10;
	private static final int BN_STATE_OF_REGsize = 2;
	private static final int BN_ABNsize = 20;
	private static final int HASH = 9999999;
	static String query;
	static int pageSize;
		
	public static void main (String[] args){

		if(args.length == 2){
			query = args[0];
			String pageSizeString = args[1];
			pageSize = Integer.parseInt(args[1]);
			try{
				pageSize = Integer.parseInt(pageSizeString);
			} catch (IndexOutOfBoundsException e){
				System.err.println("Error.");
				System.exit(0);
			}
		}else{
			System.err.println("Error.");
			System.exit(0);
		}

		long startTime = System.nanoTime();
		RandomAccessFile hashFile = null;
		RandomAccessFile heapFile = null;

		byte[] byteQuery = getByteArr(query);
		try{
			startTime = System.nanoTime();
			heapFile = new RandomAccessFile("heap."+ pageSize, "r");
			hashFile = new RandomAccessFile("hash."+ pageSize, "r");
			int initOffset = getHash(byteQuery) * INTBYTES;
			int currOffset = initOffset;

			while(true){
				hashFile.seek(currOffset);
				int hashPointer = hashFile.readInt();
				byte[] heapName = new byte[200];
				if(hashPointer > -1){
					heapFile.seek(hashPointer);
					heapFile.read(heapName);

					String value1 = new String(heapName, "UTF-8");
					if(value1.toLowerCase().contains(query.toLowerCase())){
						printRecord(heapFile, heapName, hashPointer);
					}
				}
				currOffset = currOffset + INTBYTES;
				if(currOffset > (HASH - 1) * INTBYTES){
					currOffset = 0;
				}
				if(currOffset == initOffset){
			        long stopTime = System.nanoTime();
			        long timeUsed = (stopTime - startTime) / 1000000;
					System.out.println("Time used = " + timeUsed + " milliseconds");
					break;
				}
			}
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (hashFile != null){
				try{
					hashFile.close();
				} catch (IOException e){
					e.printStackTrace();
				}
			}
			if (heapFile != null){
				try{
					heapFile.close();
				} catch (IOException e){
					e.printStackTrace();
				}
			}
		}
	}

	public static void printRecord(RandomAccessFile heapFile, byte[] heapName, int hashPointer){
		try{
			String name = new String(heapName);
			String bn_name;
			String bn_status;
			String bn_reg_dt;
			String bn_cancel_dt;
			String bn_renew_dt;
			String bn_state_num;
			String bn_state_of_reg;
			String bn_abn;

			bn_name = name;

			heapFile.seek(hashPointer + BN_NAMEsize);
			boolean bolStatus = heapFile.readBoolean();
			if(bolStatus)
				bn_status = "Registered";
			else
				bn_status = "Deregistered";

			heapFile.seek(hashPointer + BN_NAMEsize + BN_STATUSsize);
			byte[] readRegDate = new byte[BN_REG_DTsize];
			heapFile.read(readRegDate);
			bn_reg_dt = new String(readRegDate);

			heapFile.seek(hashPointer + BN_NAMEsize + BN_STATUSsize + BN_REG_DTsize);
			byte[] readCancelDate = new byte[BN_CANCEL_DTsize];
			heapFile.read(readCancelDate);
			bn_cancel_dt = new String(readCancelDate);

			heapFile.seek(hashPointer + BN_NAMEsize + BN_STATUSsize + BN_REG_DTsize + BN_CANCEL_DTsize);
			byte[] readRenewDate = new byte[BN_RENEW_DTsize];
			heapFile.read(readRenewDate);
			bn_renew_dt = new String(readRenewDate);

			heapFile.seek(hashPointer + BN_NAMEsize + BN_STATUSsize + BN_REG_DTsize + BN_CANCEL_DTsize + BN_RENEW_DTsize);
			byte[] readStateNum = new byte[BN_STATE_NUMsize];
			heapFile.read(readStateNum);
			bn_state_num = new String(readStateNum);

			heapFile.seek(hashPointer + BN_NAMEsize + BN_STATUSsize + BN_REG_DTsize + BN_CANCEL_DTsize + BN_RENEW_DTsize + BN_STATE_NUMsize);
			short shtState = heapFile.readShort();
			bn_state_of_reg = mapState.get(shtState);

			heapFile.seek(hashPointer + BN_NAMEsize + BN_STATUSsize + BN_REG_DTsize + BN_CANCEL_DTsize + BN_RENEW_DTsize +
				BN_STATE_NUMsize + BN_STATE_OF_REGsize);
			byte[] readABN = new byte[BN_ABNsize];
			heapFile.read(readABN);
			bn_abn = new String(readABN);
			
			System.out.println(bn_name + "\t" + bn_status + "\t" + bn_reg_dt + "\t" + bn_cancel_dt + "\t" + 
					bn_renew_dt + "\t" + bn_state_num + "\t" + bn_state_of_reg + "\t" + bn_abn + "\t");
			
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
	}

	//convert short int to state 
	private static final Map<Short, String> mapState = new HashMap<Short, String>();
	static{
		mapState.put((short)0, "");
		mapState.put((short)1, "NSW");
		mapState.put((short)2, "ACT");
		mapState.put((short)3, "VIC");
		mapState.put((short)4, "QLD");
		mapState.put((short)5, "SA");
		mapState.put((short)6, "WA");
		mapState.put((short)7, "TAS");
		mapState.put((short)8, "NT");
	}//Map
	
	//genereates an array that is 200 bytes long of the name
	public static byte[] getByteArr(String name){
		return Arrays.copyOf(name.getBytes(), BN_NAMEsize);
	}
	//generates a hash value of mode 3698507,
	public static int getHash(byte[] byteArray){
		return Math.abs((Arrays.hashCode(byteArray)) % HASH);
	}
}//class