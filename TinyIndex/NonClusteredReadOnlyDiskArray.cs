using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TinyIndex
{
    public class NonClusteredReadOnlyDiskArray<T> : IReadOnlyDiskArray<T>
        where T : notnull
    {
        private readonly ArrayHeader header;
        private readonly ISerializer<T> serializer;
        private readonly RandomAccessFile file;
        private readonly long pointersStartsAt;
        private readonly ICache<long, T> cache;

        private T ReadRecordAtOffset(long offset, ref byte[] buffer, out int length)
        {
            file.ReadAt(offset, buffer, 0, sizeof(int));
            var len = BitConverter.ToInt32(buffer, 0);
            Utility.EnsureArrayOfMinimalSize(ref buffer, len);
            file.ReadAt(offset + sizeof(int), buffer, 0, len);
            length = len;
            return serializer.Deserialize(buffer.AsSpan().Slice(0, len));
        }
        
        private async Task<(T result, byte[] newBuffer, int newLength)> ReadRecordAtOffsetAsync(
            long offset, byte[] buffer, CancellationToken cancellationToken = default)
        {
            await file.ReadAtAsync(offset, buffer, 0, sizeof(int), cancellationToken);
            var len = BitConverter.ToInt32(buffer, 0);
            Utility.EnsureArrayOfMinimalSize(ref buffer, len);
            await file.ReadAtAsync(offset + sizeof(int), buffer, 0, len, cancellationToken);
            int length = len;
            T result = serializer.Deserialize(buffer.AsSpan().Slice(0, len));
            return (result, buffer, length);
        }

        private T UncachedReadRecordWithId(long id, ref byte[] buffer)
        {
            var offset = OffsetFromId(id, ref buffer);
            return ReadRecordAtOffset(DataStartOffset + offset, ref buffer, out _);
        }
        
        private async Task<(T result, byte[] newBuffer)> UncachedReadRecordWithIdAsync(long id, byte[] buffer, CancellationToken cancellationToken = default)
        {
            var offset = await OffsetFromIdAsync(id, buffer, cancellationToken);
            var (result, newBuffer, _) = await ReadRecordAtOffsetAsync(DataStartOffset + offset, buffer, cancellationToken: cancellationToken);
            return (result, newBuffer);
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
        
        private async Task<(T result, byte[] newBuffer)> ReadRecordWithIdAsync(long id, byte[] buffer, CancellationToken cancellationToken = default)
        {
            byte[] newBuffer = buffer;
            var result = await cache.GetAsync(id, async () =>
            {
                T result;
                (result, newBuffer) = await UncachedReadRecordWithIdAsync(id, buffer, cancellationToken);
                return result;
            }, cancellationToken);
            return (result, newBuffer);
        }

        private long OffsetFromId(long id, ref byte[] buffer)
        {
            file.ReadAt(pointersStartsAt + id * sizeof(long), buffer, 0, sizeof(long));
            return BitConverter.ToInt64(buffer, 0);
        }
        
        private async Task<long> OffsetFromIdAsync(long id, byte[] buffer, CancellationToken cancellationToken = default)
        {
            await file.ReadAtAsync(pointersStartsAt + id * sizeof(long), buffer, 0, sizeof(long), cancellationToken);
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
                current += sizeof(int) + length;
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

        public long Count => header.RecordCount;

        private long DataStartOffset => header.StartsAt + sizeof(long);

        internal NonClusteredReadOnlyDiskArray(ArrayHeader header, RandomAccessFile file, ISerializer<T> serializer, ICache<long, T>? cache = null)
        {
            this.header = header;
            this.file = file;
            this.serializer = serializer;
            var buffer = new byte[sizeof(long)];
            file.ReadAt(header.StartsAt, buffer, 0, buffer.Length);
            pointersStartsAt = header.StartsAt + BitConverter.ToInt64(buffer, 0);
            this.cache = cache ?? new NoCache<long, T>();
        }

        public Task<T> GetAsync(long id)
        {
            if (id < 0 || id >= header.RecordCount)
                throw new ArgumentOutOfRangeException();

            return Impl(id);

            async Task<T> Impl(long id)
            {
                var buffer = new byte[sizeof(long)];
                var (result, _) = await ReadRecordWithIdAsync(id, buffer);
                return result;
            }
        }

        public async Task<(T element, long id)> BinarySearchAsync<TKey>(TKey lookupKey, Func<T, TKey> selector, IComparer<TKey> comparer)
        {
            throw new NotImplementedException();
        }

        public async Task<(long idStart, long idEndExclusive)> EqualRangeAsync<TKey>(TKey lookupKey, Func<T, TKey> selector, IComparer<TKey> comparer)
        {
            throw new NotImplementedException();
        }

        public IAsyncEnumerable<T> GetIdRangeAsync(long idStart, long idEnd)
        {
            throw new NotImplementedException();
        }

        public IAsyncEnumerable<T> LinearScanAsync()
        {
            throw new NotImplementedException();
        }

        public ValueTask<long> GetCountAsync()
        {
            return ValueTask.FromResult(header.RecordCount);
        }
    }
}