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
  // _ = HandleClient(socket); // synchronous until hits first __ await __
  _ = Task.Run(() => HandleClient(socket));
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
      Thread.Sleep(WaitMs);
      if (WaitMs == 0)
      {
        while (!_db.TryGetValue(key, out var v))
          Thread.Sleep(100);
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
      if (value.Type == RedisType.String)
        await socket.SendAsync(Encoding.UTF8.GetBytes("+string\r\n"));
      else
        await socket.SendAsync(Encoding.UTF8.GetBytes("+stream\r\n"));
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