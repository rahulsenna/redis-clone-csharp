using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using static HexdumpUtil;

byte[]? SavedBuffer = null;

int replicaAckCount = 0;
int masterWriteOffset = 0;
bool waiting = false;

ConcurrentDictionary<string, RedisValue> _db = [];
int replicaConsumeBytes = 0;
string replicationID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
List<Socket> replicas = [];
int port = 6379;
string? replicaHost = null;
for (int i = 0; i < args.Length - 1; ++i)
{
  if (args[i] == "--port")
    int.TryParse(args[i + 1], out port);

  if (args[i] == "--replicaof")
    replicaHost = args[i + 1];
}
bool isMaster = true;
if (replicaHost != null)
{
  isMaster = false;
  await Handshake();
}

const int DEFAULT_BUFFER_SIZE = 1024;
async Task Handshake()
{
  if (!(replicaHost.Split(' ') is [var host, var portStr] && int.TryParse(portStr, out int replicaPort)))
    return;
  TcpClient client = new();
  await client.ConnectAsync(host, replicaPort);
  NetworkStream stream = client.GetStream();

  byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];

  if (!await SendAndExpect(stream, buffer, "*1\r\n$4\r\nPING\r\n", "+PONG\r\n"))
    return;

  if (!await SendAndExpect(stream, buffer, $"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${port.ToString().Length}\r\n{port}\r\n", "+OK\r\n"))
    return;

  if (!await SendAndExpect(stream, buffer, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", "+OK\r\n"))
    return;

  await stream.WriteAsync(Encoding.UTF8.GetBytes("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"));
  await stream.ReadExactlyAsync(buffer, 0, 56); // FULLRESYNC
  await stream.ReadExactlyAsync(buffer, 0, 88 + 5); // empty rdb for now (TODO: check and read entire rdb file)

  _ = HandleClient(client.Client);
}
;

async Task<bool> SendAndExpect(NetworkStream stream, byte[] buffer, string sendStr, string receiveStr)
{
  await stream.WriteAsync(Encoding.UTF8.GetBytes(sendStr));
  int bytesRead = await stream.ReadAsync(buffer);
  string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
  if (response != receiveStr)
    return false;
  return true;
}


TcpListener server = new(IPAddress.Any, port);
// server.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
// server.ExclusiveAddressUse = false;
server.Start();
while (true)
{
  Socket socket = await server.AcceptSocketAsync();
  _ = HandleClient(socket); // synchronous until hits first __ await __
  // _ = Task.Run(() => HandleClient(socket));
}

async Task<int> ChunkRESP(Socket socket, byte[] buffer)
{
  int bytesRead = 0;

  while (true)
  {
    if (SavedBuffer != null)
    {
      buffer.AsSpan().Clear();
      SavedBuffer.AsSpan().CopyTo(buffer);
      bytesRead = SavedBuffer.Length;
      SavedBuffer = null;
    }
    else
      bytesRead += await socket.ReceiveAsync(buffer.AsMemory(bytesRead));

    if (bytesRead <= 0)
    {
      Console.Error.WriteLine("Disconnected");
      return 0;
    }
    if (buffer[0] != (byte)'*')
    {
      Console.Error.WriteLine("Malformed RESP");
      return 0;
    }
    int queryCountIdx = Array.IndexOf(buffer, (byte)'\r');
    if (queryCountIdx == -1)
      continue;

    if (!Utf8Parser.TryParse(buffer.AsSpan(1, queryCountIdx), out int queryCount, out _))
      return 0;

    int expectedLines = queryCount * 2 + 1;
    int foundLines = 0;
    int searchPos = 0;
    int lineBreakIdx = -1;

    while ((lineBreakIdx = Array.IndexOf(buffer, (byte)'\r', searchPos)) != -1)
    {
      searchPos = lineBreakIdx + 2;
      foundLines++;
      if (foundLines >= expectedLines)
        break;
    }

    if (foundLines < expectedLines)
      continue;
    if (searchPos < bytesRead)
      SavedBuffer = buffer[searchPos..bytesRead];
    return searchPos;
  }
}

async Task HandleClient(Socket socket)
{
  using var _ = socket; // Auto dispose at end of function 
  byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];

  bool isMulti = false;
  List<string[]> multiCommands = [];
  while (true)
  {
    int bytesRead = await ChunkRESP(socket, buffer);
    if (bytesRead <= 0) break;
    string str = Encoding.UTF8.GetString(buffer, 0, bytesRead);
    var query = str.Split("\r\n", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Where((_, idx) => idx % 2 == 0).ToArray();
    string command = query[1];
    if (command == "MULTI")
    {
      isMulti = true;
      await socket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
    }
    else if (command == "EXEC")
    {
      if (!isMulti)
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("-ERR EXEC without MULTI\r\n"));
        continue;
      }
      isMulti = false;

      if (multiCommands.Count == 0)
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("*0\r\n"));
        continue;
      }

      List<string> outputs = [];
      foreach (var q in multiCommands)
      {
        if (await HandleCommands(socket, q) is string output)
          outputs.Add(output);
      }
      multiCommands.Clear();
      string result = $"*{outputs.Count}\r\n{string.Join("", outputs)}";
      await socket.SendAsync(Encoding.UTF8.GetBytes(result.ToString()));
    }
    else if (command == "DISCARD")
    {
      if (!isMulti)
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("-ERR DISCARD without MULTI\r\n"));
        continue;
      }

      isMulti = false;
      multiCommands.Clear();
      await socket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
    }
    else if (isMulti)
    {
      multiCommands.Add(query);
      await socket.SendAsync(Encoding.UTF8.GetBytes("+QUEUED\r\n"));
    }
    else
    {
      if (isMaster && command == "SET")
      {
        masterWriteOffset += bytesRead;
        foreach (var replicaSocket in replicas)
          await replicaSocket.SendAsync(buffer.AsMemory(0, bytesRead));
      }

      if (await HandleCommands(socket, query) is string res)
        await socket.SendAsync(Encoding.UTF8.GetBytes(res));
    }
    replicaConsumeBytes += bytesRead;

  }
}

async Task<string?> HandleCommands(Socket socket, string[] query)
{
  string command = query[1];
  string key = query.Length > 2 ? query[2] : "";

  if (command == "PING")
  {
    if (!isMaster) return null;
    return "+PONG\r\n";
  }

  else if (command == "REPLCONF")
  {
    if (query[2] == "GETACK")
      return $"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${replicaConsumeBytes.ToString().Length}\r\n{replicaConsumeBytes}\r\n";

    if (waiting)
    {
      if (query.Length > 3 && long.TryParse(query[3], out long ackOffset) && ackOffset >= masterWriteOffset)
        replicaAckCount++;
      return null;
    }
    return "+OK\r\n";
  }
  else if (command == "WAIT")
  {
    if (masterWriteOffset == 0)
      return $":{replicas.Count}\r\n";

    int minReplica = Convert.ToInt32(query[2]);
    int timeoutMs = Convert.ToInt32(query[3]);

    replicaAckCount = 0;
    waiting = true;
    foreach (var sock in replicas)
      await sock.SendAsync(Encoding.UTF8.GetBytes("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"));

    await Task.Delay(timeoutMs);
    waiting = false;
    return $":{replicaAckCount}\r\n";
  }
  else if (command == "PSYNC")
  {
    await socket.SendAsync(Encoding.UTF8.GetBytes($"+FULLRESYNC {replicationID} 0\r\n"));
    byte[] emptyRdbFile = [
    0x24, 0x38, 0x38, 0x0D, 0x0A, 0x52, 0x45, 0x44, 0x49, 0x53,
    0x30, 0x30, 0x31, 0x31, 0xFA, 0x09, 0x72, 0x65, 0x64, 0x69,
    0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2E, 0x32, 0x2E,
    0x30, 0xFA, 0x0A, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x62,
    0x69, 0x74, 0x73, 0xC0, 0x40, 0xFA, 0x05, 0x63, 0x74, 0x69,
    0x6D, 0x65, 0xC2, 0x6D, 0x08, 0xBC, 0x65, 0xFA, 0x08, 0x75,
    0x73, 0x65, 0x64, 0x2D, 0x6D, 0x65, 0x6D, 0xC2, 0xB0, 0xC4,
    0x10, 0x00, 0xFA, 0x08, 0x61, 0x6F, 0x66, 0x2D, 0x62, 0x61,
    0x73, 0x65, 0xC0, 0x00, 0xFF, 0xF0, 0x6E, 0x3B, 0xFE, 0xC0,
    0xFF, 0x5A, 0xA2];
    await socket.SendAsync(emptyRdbFile);
    replicas.Add(socket);
  }

  else if (command == "ECHO")
    return $"+{query[2]}\r\n";

  else if (command == "SET")
  {
    _db[key] = new RedisValue(RedisType.String, query[3], query);
    if (!isMaster) return null;
    return "+OK\r\n";
  }
  else if (command == "GET")
  {
    if (!_db.TryGetValue(key, out var value) || value.IsExpired())
      return "$-1\r\n";
    else
      return $"+{value.Data as string}\r\n";
  }
  else if (command == "RPUSH")
  {
    if (!_db.TryGetValue(key, out var value))
    {
      var list = new LinkedList<string>();
      for (int i = 3; i < query.Length; ++i)
        list.AddLast(query[i]);

      _db[key] = new RedisValue(RedisType.List, list);
      return $":{list.Count}\r\n";
    }
    else
    {
      if (value.Data is not LinkedList<string> list) return null;
      lock (list)
      {
        for (int i = 3; i < query.Length; ++i)
          list.AddLast(query[i]);
      }
      return $":{list.Count}\r\n";
    }
  }
  else if (command == "LPUSH")
  {
    if (!_db.TryGetValue(key, out var value))
    {
      var list = new LinkedList<string>();
      for (int i = 3; i < query.Length; ++i)
        list.AddFirst(query[i]);

      _db[key] = new RedisValue(RedisType.List, list);
      return $":{list.Count}\r\n";
    }
    else
    {
      if (value.Data is not LinkedList<string> list) return null;
      lock (list)
      {
        for (int i = 3; i < query.Length; ++i)
          list.AddFirst(query[i]);
      }
      return $":{list.Count}\r\n";
    }
  }
  else if (command == "LRANGE")
  {
    if (!_db.TryGetValue(key, out var value))
      return "*0\r\n";

    if (value.Data is not LinkedList<string> list) return null;

    int beg = Convert.ToInt32(query[3]);
    int end = Math.Min(Convert.ToInt32(query[4]), list.Count - 1);
    beg = beg < 0 ? Math.Max(0, list.Count + beg) : beg;
    end = end < 0 ? Math.Max(0, list.Count + end) : end;

    StringBuilder Result = new();
    Result.Append($"*{end - beg + 1}\r\n");
    foreach (var item in list.Skip(beg).Take(end - beg + 1))
      Result.Append($"${item.Length}\r\n{item}\r\n");

    return Result.ToString();
  }
  else if (command == "LLEN")
  {
    if (!_db.TryGetValue(key, out var value))
      return ":0\r\n";


    if (value.Data is not LinkedList<string> list) return null;
    return $":{list.Count}\r\n";
  }
  else if (command == "LPOP")
  {
    if (!_db.TryGetValue(key, out var value))
      return "*0\r\n";

    if (value.Data is not LinkedList<string> list) return null;

    StringBuilder Result = new();
    int popCount = query.Length > 3 ? Convert.ToInt32(query[3]) : 1;

    if (popCount > 1)
      Result.Append($"*{popCount}\r\n");
    lock (list)
    {
      for (int i = 0; i < popCount; ++i)
      {
        Result.Append($"${list.First().Length}\r\n{list.First()}\r\n");
        list.RemoveFirst();
      }
    }
    return Result.ToString();
  }
  else if (command == "BLPOP")
  {
    int WaitMs = (int)(Convert.ToDouble(query[3]) * 1000);
    await Task.Delay(WaitMs);
    if (WaitMs == 0)
    {
      while (!_db.TryGetValue(key, out var v))
        await Task.Delay(100);
    }

    if (!_db.TryGetValue(key, out var value))
      return "*-1\r\n";

    if (value.Data is not LinkedList<string> list) return null;
    StringBuilder Result = new();

    Result.Append($"*2\r\n${key.Length}\r\n{key}\r\n");
    lock (list)
    {
      Result.Append($"${list.First().Length}\r\n{list.First()}\r\n");
      list.RemoveFirst();
    }
    return Result.ToString();
  }
  else if (command == "TYPE")
  {
    if (!_db.TryGetValue(key, out var value))
      return "+none\r\n";

    return $"+{value.Type.ToString().ToLower()}\r\n";
  }
  else if (command == "XADD")
  {
    Dictionary<string, string> fields = [];
    for (int i = 4; i < query.Length; i += 2)
      fields[query[i]] = query[i + 1];

    if (!_db.TryGetValue(key, out var value))
    {
      value = new RedisValue(RedisType.Stream, new SortedList<StreamID, StreamEntry>());
      _db[key] = value;
    }

    if (value.Data is not SortedList<StreamID, StreamEntry> stream) return null;


    long ms;
    ushort sequence = 0;
    string streamIdStr = query[3];
    string[] idParts = streamIdStr.Split('-');
    if (streamIdStr == "*")
      ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    else
    {
      ms = Convert.ToInt64(idParts[0]);
      if (idParts[1] != "*")
        sequence = Convert.ToUInt16(idParts[1]);
      else
      {
        if (ms == 0)
          sequence++;

        if (stream.Count > 0 && stream.Keys.Last().MS == ms)
          sequence = (ushort)(stream.Keys.Last().Seq + 1);
      }
    }
    StreamID streamID = new(ms, sequence);
    if (streamID.MS == 0 && streamID.Seq == 0)
      return "-ERR The ID specified in XADD must be greater than 0-0\r\n";

    if (stream.Count > 0 && streamID <= stream.Keys.Last())
      return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";

    StreamEntry entry = new()
    {
      Timestamp = ms,
      Sequence = sequence,
      Fields = fields,
    };
    stream[streamID] = entry;

    string outputStr = streamID.MS.ToString() + "-" + streamID.Seq.ToString();
    return $"${outputStr.Length}\r\n{outputStr}\r\n";
  }
  else if (command == "XRANGE")
  {
    if (!_db.TryGetValue(key, out var value))
      return "*0\r\n";

    if (value.Data is not SortedList<StreamID, StreamEntry> stream) return null;
    StreamID begin = query[3] == "-" ? new(0, 0) : new(Convert.ToInt64(query[3].Split('-')[0]), Convert.ToUInt16(query[3].Split('-')[1]));
    StreamID end = query[4] == "+" ? new(long.MaxValue, ushort.MaxValue) : new(Convert.ToInt64(query[4].Split('-')[0]), Convert.ToUInt16(query[4].Split('-')[1]));

    StringBuilder resultBuilder = new();
    int resultCount = 0;
    foreach (var (id, entry) in stream)
    {
      if (id > end)
        break;

      if (id >= begin)
      {
        resultCount++;
        resultBuilder.Append($"*2\r\n${id.ToString().Length}\r\n{id.ToString()}\r\n*{entry.Fields.Count * 2}\r\n");

        foreach (var (k, v) in entry.Fields)
          resultBuilder.Append($"${k.Length}\r\n{k}\r\n${v.Length}\r\n{v}\r\n");
      }
    }
    string result = $"*{resultCount}\r\n{resultBuilder}";
    return result;
  }
  else if (command == "XREAD")
  {
    return await Xread(query);
  }
  else if (command == "INCR")
  {
    if (!_db.TryGetValue(key, out var value))
      value = new RedisValue(RedisType.String, "0");
    if (!int.TryParse(value.Data as string, out int num))
      return "-ERR value is not an integer or out of range\r\n";

    num++;
    value = value with { Data = num.ToString() };
    _db[key] = value;
    return $":{num}\r\n";
  }
  else if (command == "INFO")
  {
    if (replicaHost != null)
      return "$10\r\nrole:slave\r\n";
    return $"$89\r\nrole:master\r\nmaster_replid:{replicationID}\r\nmaster_repl_offset:0\r\n";
  }
  return null;
}

async Task<string> Xread(string[] query)
{
  List<string> streamKeys = [];
  List<StreamID> ids = [];
  int block = -1;

  int idx = 3;
  if (query[2].Equals("BLOCK", StringComparison.OrdinalIgnoreCase))
  {
    block = Convert.ToInt32(query[3]);
    idx = 5;
  }

  int queryCount = (query.Length - idx) / 2;

  for (int i = 0; i < queryCount; ++i)
    streamKeys.Add(query[idx++]);

  StreamID maxID = new(long.MaxValue, ushort.MaxValue);

  for (int i = 0; i < queryCount; ++i)
  {
    string[] idStr = query[idx++].Split("-");
    if (idStr[0] == "$")
      ids.Add(maxID);
    else
      ids.Add(new(Convert.ToInt64(idStr[0]), Convert.ToUInt16(idStr[1])));
  }

  if (block > -1)
  {
    bool hasData = false;
    for (int i = 0; i < streamKeys.Count; ++i)
    {
      if (_db.TryGetValue(streamKeys[i], out var value) &&
      value.Data is SortedList<StreamID, StreamEntry> streams)
      {
        if (streams.Keys.Any(id => id > ids[i]))
        {
          hasData = true;
          break;
        }
        if (ids[i] == maxID)
          ids[i] = streams.Keys.Last();
      }
    }
    if (!hasData)
      await Task.Delay(block);
  }

  List<string> streamResults = [];
  while (true)
  {
    for (int i = 0; i < streamKeys.Count; ++i)
    {
      string streamKey = streamKeys[i];
      StreamID startID = ids[i];

      _db.TryGetValue(streamKey, out var value);
      if (value?.Data is not SortedList<StreamID, StreamEntry> streams)
        continue;

      List<string> entries = [];
      foreach (var (id, stream) in streams)
      {
        if (id <= startID) continue;
        string idStr = id.ToString();
        StringBuilder entryBuilder = new();
        entryBuilder.Append($"*2\r\n${idStr.Length}\r\n{idStr}\r\n*{stream.Fields.Count * 2}\r\n");

        foreach (var (k, v) in stream.Fields)
          entryBuilder.Append($"${k.Length}\r\n{k}\r\n${v.Length}\r\n{v}\r\n");

        entries.Add(entryBuilder.ToString());
      }
      if (entries.Count > 0)
      {
        StringBuilder streamBuilder = new();
        streamBuilder.Append($"*2\r\n${streamKey.Length}\r\n{streamKey}\r\n");
        streamBuilder.Append($"*{entries.Count}\r\n");
        foreach (var e in entries)
          streamBuilder.Append(e);

        streamResults.Add(streamBuilder.ToString());
      }
    }
    if (block == 0 && streamResults.Count == 0)
      await Task.Delay(100);
    else
      break;
  }

  string result = streamResults.Count > 0 ? $"*{streamResults.Count}\r\n{string.Join("", streamResults)}" : "*-1\r\n";
  return result;
}

public enum RedisType { None, String, List, Stream }

public sealed record RedisValue(
  RedisType Type,
  object? Data,
  long? ExpireAtMs
)
{
  public RedisValue(RedisType Type, object? Data, string[]? query = null)
  : this(Type, Data, GetExpiryTime(query)) { }

  public bool IsExpired() => ExpireAtMs is long && ExpireAtMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
  public static long? GetExpiryTime(string[]? query)
  {
    if (query is null) return null;
    if (query.Length > 5 && query[4].Equals("PX", StringComparison.OrdinalIgnoreCase))
      return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + Convert.ToInt64(query[5]);
    return null;
  }
};

public struct StreamEntry
{
  public long Timestamp;
  public ushort Sequence;
  public Dictionary<string, string> Fields;
}
public readonly struct StreamID : IComparable<StreamID>
{
  public readonly long MS;
  public readonly ushort Seq;

  public int CompareTo(StreamID other)
  {
    int cmp = MS.CompareTo(other.MS);
    return cmp != 0 ? cmp : Seq.CompareTo(other.Seq);
  }
  public StreamID(long ms, ushort seq)
  {
    MS = ms;
    Seq = seq;
  }
  public override string ToString() => $"{MS}-{Seq}";

  public static bool operator <(StreamID a, StreamID b) => a.CompareTo(b) < 0;
  public static bool operator >(StreamID a, StreamID b) => a.CompareTo(b) > 0;
  public static bool operator <=(StreamID a, StreamID b) => a.CompareTo(b) <= 0;
  public static bool operator >=(StreamID a, StreamID b) => a.CompareTo(b) >= 0;
  public static bool operator ==(StreamID a, StreamID b) => a.CompareTo(b) == 0;
  public static bool operator !=(StreamID a, StreamID b) => a.CompareTo(b) != 0;
}
