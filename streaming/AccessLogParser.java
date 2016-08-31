import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessLogParser implements Serializable {

  // Example Apache combined log line:
  //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048 "-" "Mozilla/4.0"
  // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size 10:referer 11:userAgent
  private static final String LOG_ENTRY_PATTERN =
      "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+) \"(.*)\" \"(.+)\"";
  private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

  public static ApacheAccessLog parseFromLogLine(String logline) {
    Matcher m = PATTERN.matcher(logline);
    m.find();
    String[] url = m.group(6).split("\\?");
    String endpoint = url[0];
    long contentSize = 0;
    if (!m.group(9).equals("-")) {
       contentSize = Long.parseLong(m.group(9));
    }
    return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
        m.group(5), endpoint, m.group(7), Integer.parseInt(m.group(8)), contentSize, m.group(10), m.group(11));

  }
}
