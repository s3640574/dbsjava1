import java.io.*;
import java.util.*;

public class dbquery{
	static String query;
	static int pageSize;

	public static void main (String[] args){

		if(args.length == 2){
			query = args[0];
			String strPage = args[1];
			try{
				pageSize = Integer.parseInt(strPage);
			} catch (IndexOutOfBoundsException e){
				System.out.println("Page size must be an integer.");
				System.exit(0);
			}
		}else{
			System.out.println("Error!.");
			System.exit(0);
		}
		long startTime = System.nanoTime();
		int BN_NAMEsize = 200;
		int BN_STATUSsize = 1;
		int BN_REG_DTsize = 10;
		int BN_CANCEL_DTsize = 10;
		int BN_RENEW_DTsize = 10;
		int BN_STATE_NUMsize = 10;
		int BN_STATE_OF_REGsize = 2;
		int BN_ABNsize = 20;
		int recordSize = BN_NAMEsize + BN_STATUSsize + BN_REG_DTsize + BN_CANCEL_DTsize + BN_RENEW_DTsize + BN_STATE_NUMsize + BN_STATE_OF_REGsize + BN_ABNsize;
		
		int pageOffset = 0;
		int recordPerPage = pageSize/recordSize;
		byte[] emptyByte = new byte[200];
		int currRec = 0;
		
		RandomAccessFile in = null;
		try{
			startTime = System.nanoTime();
			in = new RandomAccessFile("heap."+pageSize, "r");
			while(true){

				int currOffset = currRec * recordSize + pageOffset * pageSize;
				in.seek(currOffset);
				byte[] readByte = new byte[BN_NAMEsize];			
				in.read(readByte);
				String value1 = new String(readByte, "UTF-8");

				if(value1.toLowerCase().contains(query.toLowerCase())){

					String bn_name;
					String bn_status;
					String bn_reg_dt;
					String bn_cancel_dt;
					String bn_renew_dt;
					String bn_state_num;
					String bn_state_of_reg;
					String bn_abn;

					bn_name = value1;

					in.seek(currOffset + BN_NAMEsize);
					boolean bolStatus = in.readBoolean();
					if(bolStatus)
						bn_status = "Registered";
					else
						bn_status = "Deregistered";

					in.seek(currOffset + BN_NAMEsize + BN_STATUSsize);
					byte[] readRegDate = new byte[BN_REG_DTsize];
					in.read(readRegDate);
					bn_reg_dt = new String(readRegDate);
					
					in.seek(currOffset + BN_NAMEsize + BN_STATUSsize + BN_REG_DTsize);
					byte[] readCancelDate = new byte[BN_CANCEL_DTsize];
					in.read(readCancelDate);
					bn_cancel_dt = new String(readCancelDate);

					in.seek(
						currOffset + BN_NAMEsize + BN_STATUSsize + 
						BN_REG_DTsize + BN_CANCEL_DTsize);
					byte[] readRenewDate = new byte[BN_RENEW_DTsize];
					in.read(readRenewDate);
					bn_renew_dt = new String(readRenewDate);

					in.seek(
						currOffset + BN_NAMEsize + BN_STATUSsize + 
						BN_REG_DTsize + BN_CANCEL_DTsize + BN_RENEW_DTsize);
					byte[] readStateNum = new byte[BN_STATE_NUMsize];
					in.read(readStateNum);
					bn_state_num = new String(readStateNum);

					in.seek(
						currOffset + BN_NAMEsize + BN_STATUSsize + 
						BN_REG_DTsize + BN_CANCEL_DTsize + BN_RENEW_DTsize +
						BN_STATE_NUMsize);
					short shtState = in.readShort();
					bn_state_of_reg = mapState.get(shtState);
					
					in.seek(
						currOffset + BN_NAMEsize + BN_STATUSsize + 
						BN_REG_DTsize + BN_CANCEL_DTsize + BN_RENEW_DTsize +
						BN_STATE_NUMsize + BN_STATE_OF_REGsize);
					byte[] readABN = new byte[BN_ABNsize];
					in.read(readABN);
					bn_abn = new String(readABN);

					System.out.println(bn_name + "\t" + bn_status + "\t" + bn_reg_dt + "\t" + bn_cancel_dt + "\t" + 
							bn_renew_dt + "\t" + bn_state_num + "\t" + bn_state_of_reg + "\t" + bn_abn + "\t");
				}

				currRec++;
				if(currRec == recordPerPage){
					currRec = 0;
					pageOffset++;
				}
				if(Arrays.equals(readByte, emptyByte)){
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
			if (in != null){
				try{
					in.close();
				} catch (IOException e){
					e.printStackTrace();
				}
			}
		}
	}

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
	}
}