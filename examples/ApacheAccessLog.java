import java.io.Serializable;

/**
 * This class represents an Apache access combined log line.
 * See http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog implements Serializable {

  public String ipAddress;
  public String clientIdentd;
  public String userID;
  public String dateTimeString;
  public String method;
  public String endpoint;
  public String protocol;
  public int responseCode;
  public long contentSize;
  public String referer;
  public String userAgent;

  public ApacheAccessLog(String ipAddress, String clientIdentd, String userID,
                          String dateTime, String method, String endpoint,
                          String protocol, int responseCode,
                          long contentSize, String referer, String userAgent) {
    this.ipAddress = ipAddress;
    this.clientIdentd = clientIdentd;
    this.userID = userID;
    this.dateTimeString = dateTime;
    this.method = method;
    this.endpoint = endpoint;
    this.protocol = protocol;
    this.responseCode = responseCode;
    this.contentSize = contentSize;
    this.referer = referer;
    this.userAgent = userAgent;
  }

}
