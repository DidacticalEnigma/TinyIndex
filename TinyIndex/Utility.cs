using System.IO;

namespace TinyIndex
{
    internal static class Utility
    {
        public static void ReadFully(this Stream stream, byte[] buffer, int offset, int count)
        {
            int numBytesToRead = count;
            int numBytesRead = 0;
            do
            {
                int n = stream.Read(buffer, numBytesRead + offset, numBytesToRead);
                if (n == 0)
                    throw new EndOfStreamException();
                numBytesRead += n;
                numBytesToRead -= n;
            } while (numBytesToRead > 0);
        }

        public static void ReadFully(this Stream stream, byte[] buffer)
        {
            ReadFully(stream, buffer, 0, buffer.Length);
        }

        public static void Write(this Stream stream, byte[] arr)
        {
            stream.Write(arr, 0, arr.Length);
        }
    }
}