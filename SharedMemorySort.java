import java.io.*;
import java.util.*;

public class SharedMemorySort {
	static String inputFileName, outputFileName;
	
	
	static int tempFileCount = 0, countOfThreads = 4;
	static long inputFileSize, sizeOfChunk, sizeOfMemory = 100000000, startTime, endTime;
	
	
	static double timeElapsed;
	
	public static void main(String[] args) 
	{
		if(args.length == 2) {
			inputFileName = args[0];
			outputFileName = args[1];
		}
		else {
			try {
				throw new Exception("Data file name and Output file name must be specified in command line arguments");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		startTime = System.currentTimeMillis();
		ManageChunks.chunks();
		endTime = System.currentTimeMillis();
		
		timeElapsed = (double)(endTime - startTime);
		System.out.println("Total time taken for external sorting: "+ (timeElapsed/1000) + " seconds");
	}
}



class Merger extends Thread
{
	@Override
	public void run() {
		int numberOfTempFiles = SharedMemorySort.tempFileCount, index = 0, minimum;
		BufferedReader [] br = new BufferedReader[numberOfTempFiles];
		
		 
		long counter = SharedMemorySort.inputFileSize/100;
		String min = "";
		
		try {
			TreeMap<String, Integer> mappedTree = new TreeMap<String, Integer>();
			BufferedWriter bw = new BufferedWriter(new FileWriter(SharedMemorySort.outputFileName));
			
			for(int var = 0; var < numberOfTempFiles; ++var) {
				br[var] = new BufferedReader(new FileReader("temp" + SharedMemorySort.inputFileName + var));
				mappedTree.put(br[var].readLine(), var);
			}

			while(index++ < counter) {
				min = (String)mappedTree.firstKey();
				minimum = (int)mappedTree.get(min);

				bw.write(min + "\r\n");
				mappedTree.remove(min);
				
				if((min = br[minimum].readLine()) != null)
					mappedTree.put(min, minimum);
			}
			bw.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}



class ManageChunks {
	static void chunks() {
		File file = new File(SharedMemorySort.inputFileName);
		double start, end, totalTimeElapsed;
		long chunkSize;
		
		SharedMemorySort.inputFileSize = file.length();
		SharedMemorySort.sizeOfChunk = getChunkSize(SharedMemorySort.inputFileSize);
		
		System.out.println("Splitting and sorting input file "+ SharedMemorySort.inputFileName + " into chunks");
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String sentence = br.readLine();
			ArrayList<String> buffer = null;
			
			start = System.currentTimeMillis();
			
			while(sentence != null)
			{
				buffer = new ArrayList<String>();
				chunkSize = 0;
				
				while(sentence != null && ((chunkSize + sentence.length()) <= SharedMemorySort.sizeOfChunk)) {
					buffer.add(sentence);
					chunkSize = chunkSize + (sentence.length() + 2);
					sentence = br.readLine();
				}
				
				WriteData.writeToFile(buffer, SharedMemorySort.tempFileCount);
				SharedMemorySort.tempFileCount = SharedMemorySort.tempFileCount + 1;
				buffer = null;
			}
			
			end = System.currentTimeMillis();
			totalTimeElapsed = (end - start) / 1000;
			
			System.out.println("Total time to create and sort: " + totalTimeElapsed);
			System.out.println("No. of temporary files that are created: " + SharedMemorySort.tempFileCount);
			
			Thread [] thread = new Thread[SharedMemorySort.countOfThreads];
			
			for(int index = 0; index < SharedMemorySort.countOfThreads; ++index) {
				System.out.println("Merging sorted files...");
				thread[index] = new Thread(new Merger());
				thread[index].start();
			}
			
			for(int index = 0; index < SharedMemorySort.countOfThreads; ++index) {
				try {
					thread[index].join();
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			br.close();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static long getChunkSize(long fileSize)
	{
		System.gc();
		System.out.println("File Size: " + fileSize + " Memory Size: " + SharedMemorySort.sizeOfMemory + " Number of Threads: " + SharedMemorySort.countOfThreads);
		System.out.println("Free memory available: " + Runtime.getRuntime().freeMemory());
		System.out.println("Number of chunks: " + (fileSize / SharedMemorySort.sizeOfMemory) + " Chunk Size: " + SharedMemorySort.sizeOfMemory);
		
		return SharedMemorySort.sizeOfMemory;
	}
}


class MergeSort extends Thread
{
	public MergeSort() {
		
	}
	
	@Override
	public void run() {

	}

	public ArrayList<String> sortData(ArrayList<String> buffer)
	{
		TreeSet<String> sortedTree = new TreeSet<String>();
		ArrayList<String> sortedList = new ArrayList<String>();
		
		for(String str: buffer) {
			if(str.length() >= 11)
				sortedTree.add(str);
		}
		
		sortedList.addAll(sortedTree);
		return sortedList;
	}
}



class WriteData {
	static void writeToFile(ArrayList<String> buffer, int fileCount) {
		
		buffer = new MergeSort().sortData(buffer);
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("temp" + SharedMemorySort.inputFileName + fileCount)));
			
			for(String str: buffer) {
				bw.write(str);
				bw.newLine();
			}
			
			bw.flush();
			bw.close();
		}
		catch (IOException e)  {
			e.printStackTrace();
		}
	}
}