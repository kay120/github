

import java.io.File;

public class findlib {
	public static void main(String[] args)
	{
		File file = new File("lib");
		for(File f:file.listFiles())
		{
			System.out.println(" "+"lib/" + f.getName()+ " ");
		}
	}
}
