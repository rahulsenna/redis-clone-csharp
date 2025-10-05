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
  _ = HandleClient(socket);
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

    if (command == "PING")
      await socket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"));

    else if (command == "ECHO")
      await socket.SendAsync(Encoding.UTF8.GetBytes($"+{query[2]}\r\n"));

    else if (command == "SET")
    {
      string key = query[2];
      _db[key] = new RedisValue(RedisType.String, query[3], query);
      await socket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
    }
    else if (command == "GET")
    {
      string key = query[2];

      if (!_db.TryGetValue(key, out var value) || value.IsExpired())
        await socket.SendAsync(Encoding.UTF8.GetBytes("$-1\r\n"));
      else
        await socket.SendAsync(Encoding.UTF8.GetBytes($"+{value.Data as string}\r\n"));
    }
    else if (command == "RPUSH")
    {
      string key = query[2];
      if (!_db.TryGetValue(key, out var value))
      {
        var list = new List<string>();
        for (int i = 3; i < query.Length; ++i)
          list.Add(query[i]);

        _db[key] = new RedisValue(RedisType.List, list);
        await socket.SendAsync(Encoding.UTF8.GetBytes($":{list.Count}\r\n"));
      }
      else
      {
        if (value.Data is not List<string> list) continue;

        for (int i = 3; i < query.Length; ++i)
          list.Add(query[i]);
        await socket.SendAsync(Encoding.UTF8.GetBytes($":{list.Count}\r\n"));
      }
    }
    else if (command == "LRANGE")
    {
      string key = query[2];
      if (!_db.TryGetValue(key, out var value))
      {
        await socket.SendAsync(Encoding.UTF8.GetBytes("*0\r\n"));
        continue;
      }

      if (value.Data is not List<string> list) continue;

      int beg = Convert.ToInt32(query[3]);
      int end = Math.Min(Convert.ToInt32(query[4]), list.Count - 1);
      beg = beg < 0 ? Math.Max(0, list.Count + beg) : beg;
      end = end < 0 ? Math.Max(0, list.Count + end) : end;

      StringBuilder Result = new();
      for (int i = beg; i <= end; ++i)
        Result.Append($"${list[i].Length}\r\n{list[i]}\r\n");

      await socket.SendAsync(Encoding.UTF8.GetBytes($"*{end - beg + 1}\r\n{Result}"));
    }

  }
}

public enum RedisType { None, String, List }

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