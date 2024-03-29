﻿using System;
using System.Collections.Generic;

namespace TinyIndex
{
    public class ClusteredReadOnlyDiskArray<T> : ISyncReadOnlyDiskArray<T>
        where T : notnull
    {
        private readonly ArrayHeader header;
        private readonly RandomAccessFile file;
        private readonly ISerializer<T> serializer;
        private readonly int recordLength;
        private ICache<long, T> cache;

        internal static int GetRecordLength(ArrayHeader header)
        {
            var len = header.OverallLength / header.RecordCount;
            if (len > int.MaxValue || len < 1)
                throw new InvalidOperationException();
            return (int)len;
        }

        private T UncachedReadRecordWithId(long id, ref byte[] buffer)
        {
            file.ReadAt(header.StartsAt + id * recordLength, buffer, 0, recordLength);
            return serializer.Deserialize(buffer.AsSpan().Slice(0, recordLength));
        }

        private T ReadRecordWithId(long id, ref byte[] buffer)
        {
            byte[] temp = buffer;
            var result = cache.Get(id, () =>
            {
                byte[] t = temp;
                var r = UncachedReadRecordWithId(id, ref t);
                temp = t;
                return r;
            });
            buffer = temp;
            return result;
        }

        public T this[long id]
        {
            get
            {
                if (id < 0 || id >= header.RecordCount)
                    throw new ArgumentOutOfRangeException();
                var buffer = new byte[recordLength];
                return ReadRecordWithId(id, ref buffer);
            }
        }

        public IEnumerable<T> GetIdRange(long idStart, long idEnd)
        {
            if (idStart > idEnd)
                throw new ArgumentException();
            if (idStart < 0)
                throw new ArgumentOutOfRangeException();
            if (idEnd >= Count)
                throw new ArgumentOutOfRangeException();

            var count = idEnd - idStart;

            var buffer = new byte[count * recordLength];
            file.ReadAt(header.StartsAt + idStart * recordLength, buffer, 0, buffer.Length);
            var elements = new List<T>();
            for (int i = 0; i < count; ++i)
            {
                elements.Add(serializer.Deserialize(buffer.AsSpan().Slice(recordLength * i, recordLength)));
            }

            return elements;
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

        public IEnumerable<T> LinearScan()
        {
            using (var stream = file.CreateStreamAt(header.StartsAt))
            {
                var buffer = new byte[recordLength];
                for (long i = 0; i < header.RecordCount; ++i)
                {
                    stream.ReadFully(buffer);
                    yield return serializer.Deserialize(buffer.AsSpan().Slice(0, recordLength));
                }
            }
        }

        public long Count => header.RecordCount;

        internal ClusteredReadOnlyDiskArray(ArrayHeader header, RandomAccessFile file, ISerializer<T> serializer, ICache<long, T>? cache = null)
        {
            this.header = header;
            this.file = file;
            this.serializer = serializer;
            recordLength = GetRecordLength(header);
            this.cache = cache ?? new NoCache<long, T>();
        }
    }
}