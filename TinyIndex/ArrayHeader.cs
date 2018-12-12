using System;
using System.IO;

namespace TinyIndex
{
    internal class ArrayHeader
    {
        public long Version { get; } = 1;

        public long StartsAt { get; set; }

        public long EndsAt { get; set; }

        public long RecordCount { get; set; }

        public long OverallLength { get; set; }

        public object Serializer { get; set; }

        public int RecordLength
        {
            get
            {
                var len = OverallLength / RecordCount;
                if (len > int.MaxValue || len < 1)
                    throw new InvalidOperationException();
                return (int)len;
            }
        }

        public byte[] AsBytes()
        {
            using (var memoryStream = new MemoryStream())
            using (var binaryWriter = new BinaryWriter(memoryStream))
            {
                binaryWriter.Write(RecordCount);
                binaryWriter.Write(OverallLength);
                binaryWriter.Flush();
                return memoryStream.ToArray();
            }
        }
    }
}