import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;

public class SampleLogGenerator {
  public static void main(String[] args) {
    try {
      if (args.length != 2) {
        System.out.println("Usage - java SampleLogGenerator <input> <output>");
        System.exit(0);
      }
      File read = new File(args[0]);
      BufferedReader reader = new BufferedReader(new FileReader(read));
      String location = args[1];
      File f = new File(location);
      FileOutputStream writer = new FileOutputStream(f,true);
      for (;;) {
        String line = reader.readLine();
        System.out.println(line);
        writer.write((line+"\n").getBytes());
        writer.flush();
        Thread.sleep(200);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
