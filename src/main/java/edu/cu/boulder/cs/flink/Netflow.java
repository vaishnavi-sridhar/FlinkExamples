package edu.cu.boulder.cs.flink;

public class Netflow {
  public int samGeneratedId;
  public int label;
  public double timeSeconds;
  public String parseDate;
  public String dateTimeString;
  public String protocol;
  public String protocolCode;
  public String sourceIp;
  public String destIp;
  public int sourcePort;
  public int destPort;
  public String moreFragments;
  public int countFragments;
  public double durationSeconds;
  public long srcPayloadBytes;
  public long destPayloadBytes;
  public long srcTotalBytes;
  public long destTotalBytes;
  public long firstSeenSrcPacketCount;
  public long firstSeenDestPacketCount;
  public int recordForceOut;

  public Netflow(int samGeneratedId,
                 int label,
                 double timeSeconds,
                 String parseDate,
                 String dateTimeString,
                 String protocol,
                 String protocolCode,
                 String sourceIp,
                 String destIp,
                 int sourcePort,
                 int destPort,
                 String moreFragments,
                 int countFragments,
                 double durationSeconds,
                 long srcPayloadBytes,
                 long destPayloadBytes,
                 long srcTotalBytes,
                 long destTotalBytes,
                 long firstSeenSrcPacketCount,
                 long firstSeenDestPacketCount,
                 int recordForceOut)
  {
    this.label = label;;
    this.timeSeconds = timeSeconds;
    this.parseDate = parseDate;
    this.dateTimeString = dateTimeString;
    this.protocol = protocol;
    this.protocolCode = protocolCode;
    this.sourceIp = sourceIp;
    this.destIp = destIp;
    this.sourcePort = sourcePort;
    this.destPort = destPort;
    this.moreFragments = moreFragments;
    this.countFragments = countFragments;
    this.durationSeconds = durationSeconds;
    this.srcPayloadBytes = srcPayloadBytes;
    this.destPayloadBytes = destPayloadBytes;
    this.srcTotalBytes = srcTotalBytes;
    this.destTotalBytes = destTotalBytes;
    this.firstSeenSrcPacketCount = firstSeenSrcPacketCount;
    this.firstSeenDestPacketCount = firstSeenDestPacketCount;
    this.recordForceOut = recordForceOut;
  }

  public String toString()
  {
    return timeSeconds + ", " + sourceIp + ", " + destIp;
  }
}
