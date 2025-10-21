using System.Text;

public static class HexdumpUtil
{
    static string CallerFile([System.Runtime.CompilerServices.CallerFilePath] string p = "") => p;
    public static void bit_dump(byte[] data, bool file=false)
    {
        var sb = new StringBuilder(data.Length * 9); // 8 bits + space
        for (int i = 0; i < data.Length; i++)
        {
            string bits = Convert.ToString(data[i], 2).PadLeft(8, '0');
            sb.Append(bits);
            if (i + 1 < data.Length) sb.Append(' ');
        }

        if (file)
        {
           using var sw = File.AppendText(CallerFile()+".log");
            sw.WriteLine(sb.ToString());
        }
        else
            Console.Error.WriteLine(sb.ToString());
    }
    public static void Hexdump(byte[] data, bool file = false)
    {
        if (data == null)
            throw new ArgumentNullException(nameof(data));

        Hexdump(data, data.Length, file);
    }

    public static void Hexdump(byte[] data, int size, bool file = false)
    {
        if (data == null)
            throw new ArgumentNullException(nameof(data));
        if (size < 0 || size > data.Length)
            throw new ArgumentOutOfRangeException(nameof(size));

        var buffer = new StringBuilder(4096);

        for (int i = 0; i < size; i += 16)
        {
            var line = new StringBuilder(80);

            // Address part
            line.AppendFormat("{0:x8}  ", i);

            // Hex part
            for (int j = 0; j < 16; j++)
            {
                if (i + j < size)
                    line.AppendFormat("{0:x2} ", data[i + j]);
                else
                    line.Append("   ");

                if (j == 7)
                    line.Append(" ");
            }

            // ASCII part
            line.Append(" |");
            for (int j = 0; j < 16 && i + j < size; j++)
            {
                byte ch = data[i + j];
                line.Append(IsPrintable(ch) ? (char)ch : '.');
            }
            line.Append("|\n");

            // Append line to buffer
            if (buffer.Length + line.Length < 4096)
            {
                buffer.Append(line);
            }
            else
            {
                // Prevent buffer overflow
                break;
            }
        }


        if (file)
        {
            var path = CallerFile();
            File.WriteAllText(CallerFile() + ".log",
            "Idx       | Hex                                             | ASCII\n" +
            "----------+-------------------------------------------------+-----------------\n" +
            buffer.ToString() + "\n\n\n\n  ");

            // using var sw =  File.AppendText(CallerFile()+".log");
            // sw.WriteLine(
            // "Idx       | Hex                                             | ASCII\n" +
            // "----------+-------------------------------------------------+-----------------\n" +
            // buffer.ToString()+ "\n\n\n\n  ");    
        }
        else
        {
            // Print the complete output
            Console.Error.Write(
            "Idx       | Hex                                             | ASCII\n" +
            "----------+-------------------------------------------------+-----------------\n" +
            buffer.ToString());
        }


    }

    // Alternative overload for spans (more memory efficient)
    public static void Hexdump(ReadOnlySpan<byte> data)
    {
        var buffer = new StringBuilder(4096);
        int size = data.Length;

        Hexdump(data.ToArray(), data.Length);
    }

    private static bool IsPrintable(byte ch)
    {
        return ch >= 32 && ch <= 126; // Printable ASCII range
    }
}
