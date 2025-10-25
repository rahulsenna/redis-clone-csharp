using System.Buffers.Binary;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using static HexdumpUtil;


const double MIN_LATITUDE = -85.05112878;
const double MAX_LATITUDE = 85.05112878;
const double MIN_LONGITUDE = -180.0;
const double MAX_LONGITUDE = 180.0;

const double LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE;
const double LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE;
const int GEO_STEP = 26;

const double EARTH_RADIUS_IN_METERS = 6372797.560856D;

double GetDistance(double aLon, double aLat, double bLon, double bLat)
{
  double lat1Rad = double.DegreesToRadians(aLat);
  double lon1Rad = double.DegreesToRadians(aLon);
  double lat2Rad = double.DegreesToRadians(bLat);
  double lon2Rad = double.DegreesToRadians(bLon);

  double delta_lat = lat2Rad - lat1Rad;
  double delta_lon = lon2Rad - lon1Rad;

  double a = Math.Sin(delta_lat / 2.0) * Math.Sin(delta_lat / 2.0) +
         Math.Cos(lat1Rad) * Math.Cos(lat2Rad) *
           Math.Sin(delta_lon / 2.0) * Math.Sin(delta_lon / 2.0);

  double c = 2.0 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1.0 - a));
  return EARTH_RADIUS_IN_METERS * c;
}

(double, double) ConvertGridNumbersToCoordinates(uint gridLatitudeCompact, uint gridLongitudeCompact)
{
  // Calculate the grid boundaries
  double gridLatitudeMin = MIN_LATITUDE + LATITUDE_RANGE * (gridLatitudeCompact / Math.Pow(2, GEO_STEP));
  double gridLatitudeMax = MIN_LATITUDE + LATITUDE_RANGE * ((gridLatitudeCompact + 1) / Math.Pow(2, GEO_STEP));
  double gridLongitudeMin = MIN_LONGITUDE + LONGITUDE_RANGE * (gridLongitudeCompact / Math.Pow(2, GEO_STEP));
  double gridLongitudeMax = MIN_LONGITUDE + LONGITUDE_RANGE * ((gridLongitudeCompact + 1) / Math.Pow(2, GEO_STEP));

  // Calculate the center point of the grid cell
  double latitude = (gridLatitudeMin + gridLatitudeMax) / 2;
  double longitude = (gridLongitudeMin + gridLongitudeMax) / 2;

  return (longitude, latitude);
}

uint CompactInt64ToInt32(ulong v)
{
  v &= 0x5555555555555555UL;
  v = (v | (v >> 1)) & 0x3333333333333333UL;
  v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0FUL;
  v = (v | (v >> 4)) & 0x00FF00FF00FF00FFUL;
  v = (v | (v >> 8)) & 0x0000FFFF0000FFFFUL;
  v = (v | (v >> 16)) & 0x00000000FFFFFFFFUL;
  return (uint)v;
}

ulong SpreadInt32ToInt64(uint v)
{
	ulong result = v;
	result = (result | (result << 16)) & 0x0000FFFF0000FFFFUL;
	result = (result | (result << 8))  & 0x00FF00FF00FF00FFUL;
	result = (result | (result << 4))  & 0x0F0F0F0F0F0F0F0FUL;
	result = (result | (result << 2))  & 0x3333333333333333UL;
	result = (result | (result << 1))  & 0x5555555555555555UL;
	return result;
}

ulong CoordEncode(double longitude, double latitude)
{
  // Normalize to the range 0-2^26
  double normalizedLatitude = Math.Pow(2, GEO_STEP) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
  double normalizedLongitude = Math.Pow(2, GEO_STEP) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;

	// Truncate to integers
	uint latInt = (uint)normalizedLatitude;
	uint lonInt = (uint)normalizedLongitude;

	ulong lonSpread = SpreadInt32ToInt64(lonInt);
	ulong latSpread = SpreadInt32ToInt64(latInt);
	return latSpread | (lonSpread << 1);
}

(double, double) DecodeCoord(ulong geoCode)
{
  // Align bits of both latitude and longitude to take even-numbered position
  ulong y = geoCode >> 1;
  ulong x = geoCode;

  // Compact bits back to 32-bit ints
  uint gridLatitudeCompact = CompactInt64ToInt32(x);
  uint gridLongitudeCompact = CompactInt64ToInt32(y);

  return ConvertGridNumbersToCoordinates(gridLatitudeCompact, gridLongitudeCompact);
}

byte[]? SavedBuffer = null;

int replicaAckCount = 0;
int masterWriteOffset = 0;
bool waiting = false;

ConcurrentDictionary<string, SortedSet> _zsets = [];
ConcurrentDictionary<string, RedisValue> _db = [];
ConcurrentDictionary<string, HashSet<Socket>> subChannels = [];
int replicaConsumeBytes = 0;
string replicationID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
List<Socket> replicas = [];
int port = 6379;
string? replicaHost = null;

Dictionary<string, string> CONFIG = [];

for (int i = 0; i < args.Length - 1; ++i)
{
  if (args[i] == "--port")
    int.TryParse(args[i + 1], out port);

  if (args[i] == "--replicaof")
    replicaHost = args[i + 1];

  if (args[i] == "--dir")
    CONFIG["dir"] = args[i + 1];

  if (args[i] == "--dbfilename")
    CONFIG["dbfilename"] = args[i + 1];
}

if (CONFIG.ContainsKey("dir"))
{
  string rdbPath = Path.Combine(CONFIG["dir"], CONFIG["dbfilename"]);
  await Read_RDB_FILE(rdbPath);
}

bool isMaster = true;
if (replicaHost != null)
{
  isMaster = false;
  await Handshake();
}

const int DEFAULT_BUFFER_SIZE = 1024;
async Task Read_RDB_FILE(string path)
{
  if (!File.Exists(path))
    return;
  var data = File.ReadAllBytes(path);

  int byteIndex = Array.IndexOf(data, (byte)0xFB);

  int dbMapSize = data[++byteIndex];
  int dbExpiryMapSize = data[++byteIndex];

  ++byteIndex;
  for (int i = 0; i < dbMapSize; ++i)
  {
    long? timeoutMs = null;
    if (i < dbExpiryMapSize)
    {
      byte exp_type = data[byteIndex++];
      if (exp_type == 0xFC)
      {
        timeoutMs = BitConverter.ToInt64(data.AsSpan(byteIndex, 8));
        byteIndex += 8;
      }
      else if (exp_type == 0xFD)
      {
        timeoutMs = BitConverter.ToInt64(data.AsSpan(byteIndex, 4));
        byteIndex += 4;
      }
    }

    byte encoding = data[byteIndex++]; // encodings 0=string

    int length = data[byteIndex++];
    string key = Encoding.UTF8.GetString(data.AsSpan(byteIndex, length));
    byteIndex += length;

    length = data[byteIndex++];
    string value = Encoding.UTF8.GetString(data.AsSpan(byteIndex, length));
    byteIndex += length;

    _db[key] = new RedisValue(RedisType.String, value, timeoutMs);
  }

}

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
  HashSet<string> subscriptions = [];
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
    else if (command == "SUBSCRIBE")
    {
      subscriptions.Add(query[2]);
      if (subChannels.TryGetValue(query[2], out var sockets))
        sockets.Add(socket);
      else
        subChannels[query[2]] = [socket];

      await socket.SendAsync(Encoding.UTF8.GetBytes($"*3\r\n$9\r\nsubscribe\r\n${query[2].Length}\r\n{query[2]}\r\n:{subscriptions.Count}\r\n"));
    }
    else if (command == "UNSUBSCRIBE")
    {
      subscriptions.Remove(query[2]);
      if (subChannels.TryGetValue(query[2], out var sockets))
        sockets.Remove(socket);

      await socket.SendAsync(Encoding.UTF8.GetBytes($"*3\r\n$11\r\nunsubscribe\r\n${query[2].Length}\r\n{query[2]}\r\n:{subscriptions.Count}\r\n"));
    }
    else if (command == "PUBLISH")
    {
      if (subChannels.TryGetValue(query[2], out var sockets))
      {
        foreach (var sock in sockets)
          await sock.SendAsync(Encoding.UTF8.GetBytes($"*3\r\n$7\r\nmessage\r\n${query[2].Length}\r\n{query[2]}\r\n${query[3].Length}\r\n{query[3]}\r\n"));

        await socket.SendAsync(Encoding.UTF8.GetBytes($":{sockets.Count}\r\n"));
      }
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
      {
        if (subscriptions.Count > 0)
        {
          if (command == "PING")
            await socket.SendAsync(Encoding.UTF8.GetBytes("*2\r\n$4\r\npong\r\n$0\r\n\r\n"));
          else
            await socket.SendAsync(Encoding.UTF8.GetBytes($"-ERR Can't execute '{command}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"));
        }
        else
          await socket.SendAsync(Encoding.UTF8.GetBytes(res));
      }
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
  else if (command == "CONFIG")
  {
    if (query[2] == "GET")
    {
      var arg = query[3];
      return $"*2\r\n${arg.Length}\r\n{arg}\r\n${CONFIG[arg].Length}\r\n{CONFIG[arg]}\r\n";
    }
  }
  else if (command == "KEYS")
  {
    string keys = string.Join("", _db.Keys.Select(key => $"${key.Length}\r\n{key}\r\n"));
    return $"*{_db.Count}\r\n{keys}";
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
  else if (command == "ZADD")
  {
    string zKey = query[2];
    if (!_zsets.TryGetValue(zKey, out var zset))
    {
      zset = new();
      _zsets[zKey] = zset;
    }
    int count = zset.Add(query[4], double.Parse(query[3]));
    return $":{count}\r\n";
  }
  else if (command == "ZRANK")
  {
    string zKey = query[2];
    if (!_zsets.TryGetValue(zKey, out var zset))
      return "$-1\r\n";
    int? rank = zset.GetRank(query[3]);
    return rank != null ? $":{rank}\r\n" : "$-1\r\n";
  }
  else if (command == "ZRANGE")
  {
    string zKey = query[2];
    if (!_zsets.TryGetValue(zKey, out var zset))
      return "*0\r\n";
    int beg = int.Parse(query[3]);
    int end = int.Parse(query[4]);
    var members = zset.RangeByRank(beg, end).ToArray();
    string res = string.Join("", members.Select(key => $"${key.Length}\r\n{key}\r\n"));
    return $"*{members.Length}\r\n{res}";
  }
  else if (command == "ZCARD")
  {
    string zKey = query[2];
    if (!_zsets.TryGetValue(zKey, out var zset))
      return ":0\r\n";
    return $":{zset.Count}\r\n";
  }
  else if (command == "ZSCORE")
  {
    string zKey = query[2];
    if (!_zsets.TryGetValue(zKey, out var zset))
      return "$-1\r\n";
    double? score = zset.GetScore(query[3]);
    return score == null ? "$-1\r\n" : $"${score?.ToString().Length}\r\n{score}\r\n";
  }
  else if (command == "ZREM")
  {
    string zKey = query[2];
    if (!_zsets.TryGetValue(zKey, out var zset))
      return ":0\r\n";
    return zset.Remove(query[3]) ? ":1\r\n" : ":0\r\n";
  }
  else if (command == "GEOADD")
  {
    string geoKey = query[2];
    double longitude = double.Parse(query[3]);
    double latitude = double.Parse(query[4]);
    if (longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE || latitude < MIN_LATITUDE || latitude > MAX_LATITUDE)
      return "-ERR invalid longitude,latitude pair\r\n";

    if (!_zsets.TryGetValue(geoKey, out var geoSet))
    {
      geoSet = new();
      _zsets[geoKey] = geoSet;
    }
    int count = geoSet.Add(query[5], CoordEncode(longitude, latitude));
    return $":{count}\r\n";
  }
  else if (command == "GEOPOS")
  {
    StringBuilder sb = new();
    sb.Append($"*{query.Length - 3}\r\n");

    string geoKey = query[2];
    _zsets.TryGetValue(geoKey, out var geoSet);

    for (int i = 3; i < query.Length; ++i)
    {
      double? encodedPos = geoSet?.GetScore(query[i]);
      if (encodedPos == null)
        sb.Append("*-1\r\n");
      else
      {
        var (lon, lat) = DecodeCoord((ulong)encodedPos);
        string lonStr = lon.ToString();
        string latStr = lat.ToString();
        sb.Append($"*2\r\n${lonStr.Length}\r\n{lonStr}\r\n${latStr.Length}\r\n{latStr}\r\n");
      }
    }
    return sb.ToString();
  }
  else if (command == "GEODIST")
  {
    string geoKey = query[2];
    if (!_zsets.TryGetValue(geoKey, out var geoSet))
      return "$-1\r\n";

    string locationA = query[3];
    string locationB = query[4];
    double? encodedPosA = geoSet.GetScore(locationA);
    double? encodedPosB = geoSet.GetScore(locationB);

    if (encodedPosA == null || encodedPosB == null)
      return "$-1\r\n";

    var (lonA, latA) = DecodeCoord((ulong)encodedPosA);
    var (lonB, latB) = DecodeCoord((ulong)encodedPosB);

    string dist = GetDistance(lonA, latA, lonB, latB).ToString();
    return $"${dist.Length}\r\n{dist}\r\n";
  }
  else if (command == "GEOSEARCH")
  {
    string geoKey = query[2];
    if (!_zsets.TryGetValue(geoKey, out var geoSet))
      return "$-1\r\n";

    double searchLon = double.Parse(query[4]);
    double searchLat = double.Parse(query[5]);
    double searchRadius = double.Parse(query[7]);
    List<string> found = [];
    foreach (var (location, encodedPos) in geoSet._dict)
    {
      var (lon, lat) = DecodeCoord((ulong)encodedPos);
      if (GetDistance(searchLon, searchLat, lon, lat) < searchRadius)
        found.Add(location);
    }
    string res = string.Join("", found.Select(x => $"${x.Length}\r\n{x}\r\n"));
    return $"*{found.Count}\r\n{res}";
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


public class SortedSet
{
  public Dictionary<string, double> _dict = [];
  SortedDictionary<double, HashSet<string>> _sorted = [];

  public int Count => _dict.Count;
  public int Add(string member, double score)
  {
    int res = 1;
    if (_dict.TryGetValue(member, out var oldScore))
    {
      _dict.Remove(member);
      _sorted.Remove(oldScore);
      res = 0;
    }
    _dict[member] = score;
    if (!_sorted.TryGetValue(score, out var sorted))
    {
      sorted = [];
      _sorted[score] = sorted;
    }
    sorted.Add(member);
    return res;
  }
  public bool Remove(string member)
  {
    if (!_dict.TryGetValue(member, out var score))
      return false;

    _dict.Remove(member);
    _sorted.Remove(score);
    return true;
  }
  public int? GetRank(string member)
  {
    if (!_dict.TryGetValue(member, out var score))
      return null;

    int rank = 0;
    foreach (var item in _sorted)
    {
      if (item.Key < score)
        rank += item.Value.Count;
      else if (item.Key == score)
      {
        var ordered = item.Value.OrderBy(e => e, StringComparer.Ordinal);
        foreach (var mem in ordered)
        {
          if (mem == member)
            return rank;
          rank++;
        }
      }
      else
        break;
    }
    return rank;
  }
  public double? GetScore(string member) => _dict.TryGetValue(member, out var score) ? score : null;

  public IEnumerable<string> RangeByRank(int beg, int end)
  {
    beg = beg < 0 ? _dict.Count + beg : beg;
    end = end < 0 ? _dict.Count + end : end;
    end = int.Min(end, _dict.Count);

    int rank = 0;
    foreach (var item in _sorted)
    {
      if (rank + item.Value.Count < beg)
      {
        rank += item.Value.Count;
        continue;
      }
      var ordered = item.Value.OrderBy(x => x, StringComparer.Ordinal);
      foreach (var v in ordered)
      {
        if (rank < beg)
        {
          rank++;
          continue;
        }
        if (rank > end)
          yield break;
        yield return v;
        rank++;
      }
    }
  }
}