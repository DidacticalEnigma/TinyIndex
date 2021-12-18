using System.IO;

namespace TinyIndex
{
    internal class ArrayHeader
    {
        public ArrayHeader(object serializer)
        {
            this.Serializer = serializer;
        }
        
        public long StartsAt { get; set; }

        public long EndsAt { get; set; }

        public long RecordCount { get; set; }

        public long OverallLength { get; set; }

        public long Type { get; set; }

        public object Serializer { get; }

        public byte[] AsBytes()
        {
            using (var memoryStream = new MemoryStream())
            using (var binaryWriter = new BinaryWriter(memoryStream))
            {
                binaryWriter.Write(RecordCount);
                binaryWriter.Write(OverallLength);
                binaryWriter.Write(Type);
                binaryWriter.Flush();
                return memoryStream.ToArray();
            }
        }
    }
}