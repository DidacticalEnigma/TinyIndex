using System;
using System.Collections.Generic;

namespace TinyIndex
{
    public class ClusteredReadOnlyDiskArray<T> : IReadOnlyDiskArray<T>
    {
        private readonly ArrayHeader header;
        private readonly RandomAccessFile file;
        private readonly ISerializer<T> serializer;

        public T this[long id]
        {
            get
            {
                if (id < 0 || id >= header.RecordCount)
                    throw new ArgumentOutOfRangeException();
                var buffer = new byte[header.RecordLength];
                file.ReadAt(header.StartsAt + id*header.RecordLength, buffer, 0, buffer.Length);
                return serializer.Deserialize(buffer, 0);
            }
        }

        public IEnumerable<T> GetIdRange(long idStart, long idEnd)
        {
            if (idStart > idEnd)
                throw new ArgumentException();
            if (idStart < 0)
                throw new ArgumentOutOfRangeException();
            if (idEnd >= header.RecordCount)
                throw new ArgumentOutOfRangeException();

            var count = idEnd - idStart;

            var buffer = new byte[count * header.RecordLength];
            file.ReadAt(header.StartsAt + idStart * header.RecordLength, buffer, 0, buffer.Length);
            var elements = new List<T>();
            for (int i = 0; i < count; ++i)
            {
                elements.Add(serializer.Deserialize(buffer, header.RecordLength * i));
            }

            return elements;
        }

        // this relies the collection is sorted on TKey
        public (T element, long id) BinarySearch<TKey>(Func<T, TKey> selector)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<T> LinearScan()
        {
            using (var stream = file.CreateStreamAt(header.StartsAt))
            {
                var buffer = new byte[header.RecordLength];
                for (long i = 0; i < header.RecordCount; ++i)
                {
                    stream.ReadFully(buffer);
                    yield return serializer.Deserialize(buffer, 0);
                }
            }
        }

        internal ClusteredReadOnlyDiskArray(ArrayHeader header, RandomAccessFile file, ISerializer<T> serializer)
        {
            this.header = header;
            this.file = file;
            this.serializer = serializer;
        }
    }
}