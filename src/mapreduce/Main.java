package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
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
	final static String output = "F:\\outMap.txt";

	final static String output1 = "F:\\outMapSorted.txt";
	final static String location = "F:\\со c++\\map\\";
	final static String locationMap = "F:\\со c++\\map\\Debug\\map.exe";
	final static String locationDeruce = "F:\\со c++\\reduce\\Debug\\";
	public static void main(String[] args) throws IOException {
		ProcessBuilder bd = new ProcessBuilder(locationMap);
		bd.redirectInput(new File (input));
		bd.redirectOutput(new File(output));
        Process process = bd.start();
        try {
			process.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        ArrayList<Item> items = new ArrayList<>();
        File file = new File(output);
        try{
			BufferedReader reader = new BufferedReader(new FileReader(file.getAbsoluteFile()));
			String str[];
			String temp;
			while ((temp = reader.readLine()) != null) {
				str = temp.split("\t");
				items.add(new Item(str[0], Integer.parseInt(str[1])));
			}
			reader.close();
		} catch(IOException e) {e.printStackTrace(); }
		items.sort(new Comparator<Item>() {
			@Override
			public int compare(Item o1, Item o2) {
				return o1.getT().compareTo(o2.getT());
			}
		});
		File out = new File(output1);
		try {
			PrintWriter writer = new PrintWriter(out.getAbsoluteFile());
			for (int i = 0; i < items.size(); i++) {
			    writer.println(items.get(i).getT() + "\t" + items.get(i).getVal());
			}
			writer.close();
		} catch (FileNotFoundException e) { }
    }
}
