using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;


const int DEFAULT_BUFFER_SIZE = 1024;

TcpListener server = new(IPAddress.Any, 6379);
// server.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
// server.ExclusiveAddressUse = false;
server.Start();
ConcurrentDictionary<string, RedisValue> _db = [];
while (true)
{
  Socket socket = await server.AcceptSocketAsync();
  _ = HandleClient(socket); // synchronous until hits first __ await __
  // _ = Task.Run(() => HandleClient(socket));
}

async Task HandleClient(Socket socket)
{
  using var _ = socket; // Auto dispose at end of function 
  byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
  while (true)
  {
    int bytesRead = await socket.ReceiveAsync(buffer);
    if (bytesRead <= 0) break;

    string str = Encoding.UTF8.GetString(buffer, 0, bytesRead);
    var query = str.Split("\r\n", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Where((_, idx) => idx % 2 == 0).ToArray();
    string command = query[1];
    string key = query.Length > 2 ? query[2] : "";

    if (command == "PING")
      await socket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"));

    else if (command == "ECHO")
      await socket.SendAsync(Encoding.UTF8.GetBytes($"+{query[2]}\r\n"));

    else if (command == "SET")
    {
      _db[key] = new RedisValue(RedisType.String, query[3], query);
      await socket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
    }
    else if (command == "GET")
    {
      if (!_db.TryGetValue(key, out var value) || value.IsExpired())
        await socket.SendAsync(Encoding.UTF8.GetBytes("$-1\r\n"));
      else
        await socket.SendAsync(Encoding.UTF8.GetBytes($"+{value.Data as string}\r\n"));
    }
    else if (command == "RPUSH")
    {
      if (!_db.TryGetValue(key, out var value))
      {
        var list = new LinkedList<string>();
        for (int i = 3; i < query.Length; ++i)
          list.AddLast(query[i]);

        _db[key] = new RedisValue(RedisType.List, list);
        await socket.SendAsync(Encoding.UTF8.GetBytes($":{list.Count}\r\n"));
      }
      else
      {
        if (value.Data is not LinkedList<string> list) continue;
        lock (list)
        {
          for (int i = 3; i < query.Length; ++i)
            list.AddLast(query[i]);
        }
        await socket.SendAsync(Encoding.UTF8.GetBytes($":{list.Count}\r\n"));
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
        await socket.SendAsync(Encoding.UTF8.GetBytes($":{list.Count}\r\n"));
      }
      else
      {
        if (value.Data is not LinkedList<string> list) continue;
        lock (list)
        {
          for (int i = 3; i < query.Length; ++i)
            list.AddFirst(query[i]);
        }
        await socket.SendAsync(Encoding.UTF8.GetBytes($":{list.Count}\r\n"));
      }
    }
    else if (command == "LRANGE")
    {
      if (!_db.TryGetValue(key, out var value))
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("*0\r\n"));
        continue;
      }

      if (value.Data is not LinkedList<string> list) continue;

      int beg = Convert.ToInt32(query[3]);
      int end = Math.Min(Convert.ToInt32(query[4]), list.Count - 1);
      beg = beg < 0 ? Math.Max(0, list.Count + beg) : beg;
      end = end < 0 ? Math.Max(0, list.Count + end) : end;

      StringBuilder Result = new();
      Result.Append($"*{end - beg + 1}\r\n");
      foreach (var item in list.Skip(beg).Take(end - beg + 1))
        Result.Append($"${item.Length}\r\n{item}\r\n");

      await socket.SendAsync(Encoding.UTF8.GetBytes(Result.ToString()));
    }
    else if (command == "LLEN")
    {
      if (!_db.TryGetValue(key, out var value))
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes(":0\r\n"));
        continue;
      }

      if (value.Data is not LinkedList<string> list) continue;
      await socket.SendAsync(Encoding.UTF8.GetBytes($":{list.Count}\r\n"));
    }
    else if (command == "LPOP")
    {
      if (!_db.TryGetValue(key, out var value))
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("*0\r\n"));
        continue;
      }
      if (value.Data is not LinkedList<string> list) continue;

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
      await socket.SendAsync(Encoding.UTF8.GetBytes(Result.ToString()));
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
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("*-1\r\n"));
        continue;
      }

      if (value.Data is not LinkedList<string> list) continue;
      StringBuilder Result = new();

      Result.Append($"*2\r\n${key.Length}\r\n{key}\r\n");
      lock (list)
      {
        Result.Append($"${list.First().Length}\r\n{list.First()}\r\n");
        list.RemoveFirst();
      }
      await socket.SendAsync(Encoding.UTF8.GetBytes(Result.ToString()));
    }
    else if (command == "TYPE")
    {
      if (!_db.TryGetValue(key, out var value))
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("+none\r\n"));
        continue;
      }
      await socket.SendAsync(Encoding.UTF8.GetBytes($"+{value.Type.ToString().ToLower()}\r\n"));
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

      if (value.Data is not SortedList<StreamID, StreamEntry> stream) continue;


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
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
        continue;
      }
      if (stream.Count > 0 && streamID <= stream.Keys.Last())
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
        continue;
      }

      StreamEntry entry = new()
      {
        Timestamp = ms,
        Sequence = sequence,
        Fields = fields,
      };
      stream[streamID] = entry;

      string outputStr = streamID.MS.ToString() + "-" + streamID.Seq.ToString();
      await socket.SendAsync(Encoding.UTF8.GetBytes($"${outputStr.Length}\r\n{outputStr}\r\n"));
    }
    else if (command == "XRANGE")
    {
      if (!_db.TryGetValue(key, out var value))
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("*0\r\n"));
        continue;
      }

      if (value.Data is not SortedList<StreamID, StreamEntry> stream) continue;
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
      await socket.SendAsync(Encoding.UTF8.GetBytes(result));
    }
    else if (command == "XREAD")
    {
      await Xread(query, socket);
    }

  }
}

async Task Xread(string[] query, Socket socket)
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
  await socket.SendAsync(Encoding.UTF8.GetBytes(result));
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
