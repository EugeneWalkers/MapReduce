package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;

class Item{
	private String t;
	private int val;
	public Item(String _t, int _val){
		t = _t;
		val = _val;
	}
	public String getT() {
		return t;
	}
	public void setT(String t) {
		this.t = t;
	}
	public int getVal() {
		return val;
	}
	public void setVal(int val) {
		this.val = val;
	}
}

public class Main {
	final static String input = "F:\\in.txt";
	final static String output = "F:\\out.txt";
	final static String locationMap = "F:\\УП c++\\map\\Debug\\map.exe";
	final static String locationReduce = "F:\\УП c++\\reduce\\Debug\\reduce.exe";
	public static void main(String[] args) throws IOException {
		String str, str1, str2, key = null;
		int num = 1, byteSize = 4096, numFiles = 0, curNumFiles = 0, copyCurNumFiles, value = 0;
		byte curByte = 1, buffer;
		String[] words, arrSplit1, arrSplit2;
		byte[] arrByte, endArrByte = new byte[byteSize];
		File in, out = null, file1, file2, fileMerge = null, finalFile;
		RandomAccessFile fileBeforeMap;
		RandomAccessFile fileAfterMerge;
		StringBuffer strBuf, endStrBuf;
		BufferedReader reader, endReader = null;
		PrintWriter writer, endWriter, bufWriter;
		ProcessBuilder pb_map, pb_reduce;
		Process proc_map, proc_reduce;
		ArrayList<Item> store;
        fileMerge = new File("");
		fileBeforeMap = new RandomAccessFile(input, "r");
		while (num <= fileBeforeMap.length()) {
			numFiles++;
			fileBeforeMap.seek(--num);
			strBuf = new StringBuffer("");
			out = new File(String.valueOf(numFiles) + ".txt");
			writer = new PrintWriter(out.getAbsoluteFile());
			arrByte = new byte[byteSize];
			fileBeforeMap.read(arrByte);
			for (int i = 0; i < arrByte.length; i++) {
				strBuf.append((char) arrByte[i]);
			}
			num += byteSize;
			while(curByte == 1 && num <= fileBeforeMap.length()){
				fileBeforeMap.seek(num++);
				curByte = fileBeforeMap.readByte();
				if ((curByte > 64 && curByte < 91) || (curByte > 96 && curByte < 123)) {
					strBuf.append((char)curByte);
					curByte = 1;
				}
			}
			curByte = 1;
			writer.print(strBuf);
			writer.close();
		}
        fileBeforeMap.close();
        for (int w = 1; w <= numFiles; w++) {
        	pb_map = new ProcessBuilder(locationMap);
        	in = new File (String.valueOf(w) + ".txt");
        	out = new File (String.valueOf(w) + "_merge.txt");
        	pb_map.redirectInput(in);
        	pb_map.redirectOutput(out);
        	proc_map = pb_map.start();
        	try { 
        		proc_map.waitFor(); 
        	} catch (InterruptedException e) {e.printStackTrace();}
        	in.delete();
        }
        
        // mergesort
        
        for (int w = 1; w <= numFiles; w++) {
        	store = new ArrayList<>();
        	in = new File(String.valueOf(w) + "_merge.txt");
        	try{
        		reader = new BufferedReader(new FileReader(in.getAbsoluteFile()));
        		while ((str = reader.readLine()) != null) {
        			words = str.split("\t");
        			store.add(new Item(words[0], Integer.parseInt(words[1])));
        		}
        		reader.close();
        		in.delete();
        	} catch(IOException e) {System.out.print("Error"); }
        	store.sort(new Comparator<Item>() {
        		public int compare(Item o1, Item o2) {
        			return o1.getT().compareTo(o2.getT());
        		}
        	});
        	out = new File(String.valueOf(w) + "_merge_1.txt");
        	try {
        		writer = new PrintWriter(out.getAbsoluteFile());
        		for (int i = 0; i < store.size(); i++) {
        			writer.println(store.get(i).getT() + "\t" + store.get(i).getVal());
        		}
        		writer.close();
        	} catch (FileNotFoundException e) { }
        }
        curNumFiles = numFiles;
        for (int e = 1; e <= numFiles; e *= 2) {
        	copyCurNumFiles = curNumFiles / 2;
        	curNumFiles = (curNumFiles + 1) / 2;
        	for (int w = 1; w <= curNumFiles; w++) {
        		file1 = new File(String.valueOf(2 * w - 1) + "_merge_" + String.valueOf(e) + ".txt");
        		if (w == curNumFiles && copyCurNumFiles != curNumFiles) {
        			file1.renameTo(new File(String.valueOf(w) + "_merge_" + String.valueOf(e * 2) + ".txt"));
        			break;
        		}
        		file2 = new File(String.valueOf(2 * w) + "_merge_" + String.valueOf(e) + ".txt");
        		fileMerge = new File(String.valueOf(w) + "_merge_" + String.valueOf(e * 2) + ".txt");
        		BufferedReader reader1 = new BufferedReader(new FileReader(file1.getAbsoluteFile()));
        		BufferedReader reader2 = new BufferedReader(new FileReader(file2.getAbsoluteFile()));
        		writer = new PrintWriter(fileMerge.getAbsoluteFile());
        		str1 = reader1.readLine();
        		str2 = reader2.readLine();
        		if (str1 != null && str2 != null) { 
        			arrSplit1 = str1.split("\t");
            		arrSplit2 = str2.split("\t");
        			while (str1 != null && str2 != null) {
        				if (arrSplit1[0].compareTo(arrSplit2[0]) <= 0) {
        					writer.println(str1);
        					if ((str1 = reader1.readLine()) != null) {
        						arrSplit1 = str1.split("\t");
        					}
        				} else {
        					writer.println(str2);
        					if ((str2 = reader2.readLine()) != null) {
        						arrSplit2 = str2.split("\t");
        					}
        				}
        			}
        		}
        		if (str1 == null) {
        			writer.println(str2);
        			while ((str2 = reader2.readLine()) != null) {
        				writer.println(str2);
        			}
        		} else if (str2 == null) {
        			writer.println(str1);
        			while ((str1 = reader1.readLine()) != null) {
        				writer.println(str1);
        			}
        		}
        		writer.close();
        		reader1.close();
        		reader2.close();
        		file1.delete();
        		file2.delete();
        	}
        }
        
        
        //снова дробление файла и выполнение для них редьюс с последующим слиянием

        finalFile = new File(output);
        fileAfterMerge = new RandomAccessFile(fileMerge.getName(), "r");
        endWriter = new PrintWriter(finalFile.getAbsoluteFile());
        while ((int)(buffer = (byte) fileAfterMerge.read(endArrByte)) != -1) {
            endStrBuf = new StringBuffer("");
            bufWriter = new PrintWriter(new File("buffer.txt").getAbsoluteFile());
        	for (int i = 0; i < endArrByte.length; i++) {
        		endStrBuf.append((char)endArrByte[i]);
        	}
        	if (buffer == 0) {
        		while ((char)(buffer = fileAfterMerge.readByte()) != '\n') {
        			endStrBuf.append((char)buffer);
        		}
            	endStrBuf.append((char)buffer);
        	}
        	bufWriter.print(endStrBuf);
        	bufWriter.close();
        	pb_reduce = new ProcessBuilder(locationReduce);
        	in = new File ("buffer.txt");
        	out = new File ("buffer1.txt");
        	pb_reduce.redirectInput(in);
        	pb_reduce.redirectOutput(out);
        	proc_reduce = pb_reduce.start();
        	try { 
        		proc_reduce.waitFor(); 
        	} catch (InterruptedException e) {e.printStackTrace();}
        	in.delete();
        	endReader = new BufferedReader(new FileReader(out.getAbsoluteFile()));
        	endStrBuf = new StringBuffer("");
        	while ((str = endReader.readLine()) != null) {
        		endStrBuf.append(str + "\n");
        	}
        	arrSplit1 = endStrBuf.toString().split("\n");
        	arrSplit2 = arrSplit1[0].split("\t");
        	if (arrSplit2[0].equals(key)) {
        		value += Integer.parseInt(arrSplit2[1]);
        		if (arrSplit1.length > 1) {
            		if (key != null) {
            			endWriter.println(key + "\t" + value);
            		}
        			for (int h = 1; h < arrSplit1.length - 1; h++) {
        				endWriter.println(arrSplit1[h]);
        			}
        			arrSplit2 = arrSplit1[arrSplit1.length - 1].split("\t");
        			key = arrSplit2[0];
        			value = Integer.parseInt(arrSplit2[1]);
        		}
        	} else {
        		if (key != null) {
        			endWriter.println(key + "\t" + value);
        		}
        		if (arrSplit1.length > 1) {
        			for (int h = 0; h < arrSplit1.length - 1; h++) {
        				endWriter.println(arrSplit1[h]);
        			}
        		}
        		arrSplit2 = arrSplit1[arrSplit1.length - 1].split("\t");
    			key = arrSplit2[0];
    			value = Integer.parseInt(arrSplit2[1]);
        	}
        	endReader.close();
            out.delete();
        }
        if (key != null) {
        	endWriter.print(key + "\t" + value);
        }
        endWriter.close();
        fileAfterMerge.close();
        fileMerge.delete();
	}

}
