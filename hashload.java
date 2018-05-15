import java.io.*;
import java.util.*;

public class hashload{
	private static final int NAMESIZE = 200;
	private static final int RECSIZE = 263;
	private static final int HASH = 9999999;
	private static final int INTBYTES = 4;
	static int pageSize;

	public static void main (String[] args){

		if(args.length == 1){
			String pageSizeString = args[0];
			try{
				pageSize = Integer.parseInt(pageSizeString);
			} catch (IndexOutOfBoundsException e){
				System.out.println("Error.");
				System.exit(0);
			}
		}else{
			System.out.println("Error.");
			System.exit(0);
		}

		int pageOffset = 0;
		int recordPerPage = pageSize/RECSIZE;
		int currRec = 0;
		byte[] emptyByte = new byte[200];
		long startTime = System.nanoTime();
		
		RandomAccessFile heapFile = null;
		RandomAccessFile hashFile = null;
		try{
			startTime = System.nanoTime();
			heapFile = new RandomAccessFile("heap."+pageSize, "r");
			hashFile = new RandomAccessFile("hash."+pageSize, "rw");

			for(int i = 0; i < HASH; i++)
				hashFile.writeInt(-1);

			while(true){
				int currOffset = currRec * RECSIZE + pageOffset * pageSize;
				heapFile.seek(currOffset);
				byte[] readByte = new byte[NAMESIZE];
				heapFile.read(readByte);

				if(Arrays.equals(readByte, emptyByte)){
					System.out.println("HeapFile is empty.");
					break;
				}

				int hashName = getHash(readByte) * INTBYTES;
				int hashOffset = hashName;
				while(true){
					hashFile.seek(hashOffset);
					int bucket = hashFile.readInt();
					hashFile.seek(hashOffset);

					if(bucket == -1){
						hashFile.writeInt(currOffset);
						break;
					}else{
						hashOffset = hashOffset + INTBYTES;
						if(hashOffset > (HASH - 1) * INTBYTES){
							hashOffset = 0;
						}
						if(hashOffset == hashName){
							System.err.println("Error. HashFile is full.");
							System.exit(0);
						}
					}
				}
				currRec++;
				if(currRec == recordPerPage){
					currRec = 0;
					pageOffset++;
				}
			}
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (heapFile != null){
				try{
					heapFile.close();
			        long stopTime = System.nanoTime();
			        long timeUsed = (stopTime - startTime) / 1000000;
					System.out.println("Time used = " + timeUsed + " milliseconds");
				} catch (IOException e){
					e.printStackTrace();
				}
			}
		}
	}
	
	public static byte[] getByteArr(String name){
		return Arrays.copyOf(name.getBytes(), NAMESIZE);
	}
	public static int getHash(byte[] byteArray){
		return Math.abs((Arrays.hashCode(byteArray)) % HASH);
	}
}