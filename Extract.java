import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class Extract {
	public static void main(String[] args) {
		
		try {
            // Path dir = Paths.get("/home/junwata/HiBench/my-data");
            // Stream<Path> stream = Files.list(dir);
            // stream.forEach(p -> System.out.println(p.toString()));
			File file_read = new File("/home/junwata/HiBench/my-data/slow1-dataSet650000-interval2000-notIncremental/jobmanager.log");
            File file_write = new File("formatted.csv");
			
			if (file_read.exists()) {
				FileReader fr = new FileReader(file_read);
				BufferedReader br = new BufferedReader(fr);
				String content;

                FileWriter fw = new FileWriter(file_write);

                String keyword = "Completed checkpoint";

                fw.write("chknum,jobid,chksiz[bytes],chkdur[ms]\n");
				while ((content = br.readLine()) != null) {
                    if (content.indexOf(keyword) != -1) {
                        String[] words = content.split(" ");
                        // for (int i = 0; i < words.length; i++) {
                        //     fw.write("[" + i + "]: " + words[i] + "   ");
                        // }
                        fw.write(words[12] + "," + words[15] + "," + words[16].replace("(", "") + "," + words[18].replace("checkpointDuration=", ""));
                        fw.write("\n");
                        // fw.write(content + "\n");
                    }
				}
				br.close();
                fw.close();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
