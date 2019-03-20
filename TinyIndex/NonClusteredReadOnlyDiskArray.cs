using System;
using System.Collections.Generic;

namespace TinyIndex
{
    public class NonClusteredReadOnlyDiskArray<T> : IReadOnlyDiskArray<T>
    {
        private ArrayHeader header;
        private ISerializer<T> serializer;
        private RandomAccessFile file;
        private long pointersStartsAt;

        private T ReadRecordAtOffset(long offset, ref byte[] buffer, out int length)
        {
            file.ReadAt(offset, buffer, 0, sizeof(int));
            var len = BitConverter.ToInt32(buffer, 0);
            Utility.EnsureArrayOfMinimalSize(ref buffer, len);
            file.ReadAt(offset + sizeof(int), buffer, 0, len);
            length = len;
            return serializer.Deserialize(buffer, 0, len);
        }

        private T ReadRecordWithId(long id, ref byte[] buffer)
        {
            var offset = OffsetFromId(id, ref buffer);
            return ReadRecordAtOffset(DataStartOffset + offset, ref buffer, out _);
        }

        private long OffsetFromId(long id, ref byte[] buffer)
        {
            file.ReadAt(pointersStartsAt + id * sizeof(long), buffer, 0, buffer.Length);
            return BitConverter.ToInt64(buffer, 0);
        }

        public T this[long id]
        {
            get
            {
                if (id < 0 || id >= header.RecordCount)
                    throw new ArgumentOutOfRangeException();
                var buffer = new byte[sizeof(long)];
                return ReadRecordWithId(id, ref buffer);
            }
        }

        // this relies the collection is sorted on TKey
        public (T element, long id) BinarySearch<TKey>(TKey lookupKey, Func<T, TKey> selector, IComparer<TKey> comparer)
        {
            return Utility.BinarySearch(
                ReadRecordWithId,
                header.RecordCount,
                lookupKey,
                selector,
                comparer);
        }

        // this relies the collection is sorted on TKey
        public (long idStart, long idEndExclusive) EqualRange<TKey>(
            TKey lookupKey,
            Func<T, TKey> selector,
            IComparer<TKey> comparer)
        {
            return Utility.EqualRange(ReadRecordWithId, header.RecordCount, lookupKey, selector, comparer);
        }

        public IEnumerable<T> GetIdRange(long idStart, long idEnd)
        {
            if (idStart > idEnd)
                throw new ArgumentException();
            if (idStart < 0)
                throw new ArgumentOutOfRangeException();
            if (idEnd >= header.RecordCount)
                throw new ArgumentOutOfRangeException();

            var buffer = new byte[sizeof(long)];
            long current = DataStartOffset + OffsetFromId(idStart, ref buffer);
            for (long i = idStart; i < idEnd; ++i)
            {
                yield return ReadRecordAtOffset(current, ref buffer, out var length);
                current += sizeof(int) + length;;
            }
        }

        public IEnumerable<T> LinearScan()
        {
            var buffer = new byte[sizeof(long)];
            long current = DataStartOffset;
            while (current < pointersStartsAt)
            {
                yield return ReadRecordAtOffset(current, ref buffer, out var length);
                current += sizeof(int) + length;
            }
        }

        private long DataStartOffset => header.StartsAt + sizeof(long);

        internal NonClusteredReadOnlyDiskArray(ArrayHeader header, RandomAccessFile file, ISerializer<T> serializer)
        {
            this.header = header;
            this.file = file;
            this.serializer = serializer;
            var buffer = new byte[sizeof(long)];
            file.ReadAt(header.StartsAt, buffer, 0, buffer.Length);
            this.pointersStartsAt = header.StartsAt + BitConverter.ToInt64(buffer, 0);

        }
    }
}