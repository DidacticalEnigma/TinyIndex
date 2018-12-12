using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace TinyIndex
{
    public class ReadOnlyDiskArray<T>
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

        internal ReadOnlyDiskArray(ArrayHeader header, RandomAccessFile file, ISerializer<T> serializer)
        {
            this.header = header;
            this.file = file;
            this.serializer = serializer;
        }
    }

    public class Database : IDisposable
    {
        private readonly IReadOnlyList<ArrayHeader> headers;
        private readonly RandomAccessFile file;

        public ReadOnlyDiskArray<T> Get<T>(int collectionNumber)
        {
            var header = headers[collectionNumber];
            return new ReadOnlyDiskArray<T>(header, file, (ISerializer<T>)header.Serializer);
        }

        public static DatabaseOpeningBuilder Open(string path)
        {
            return new DatabaseOpeningBuilder(OpenReadonly(path), () => OpenReadonly(path));
        }

        public static DatabaseBuilder CreateOrOpen(string path)
        {
            try
            {
                // the created stream is kept for the purposes of
                // avoiding TOCTTOU issues, and closed as soon
                // as possible, in the builder's Build function
                return new DatabaseCreationOrOpenBuilder(OpenReadonly(path), () => OpenReadonly(path));
            }
            catch (FileNotFoundException)
            {
                return Create(path);
            }
        }

        public static DatabaseBuilder Create(string path)
        {
            return new DatabaseCreationBuilder(path);
        }

        internal static FileStream OpenReadonly(string path)
        {
            return new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read);
        }

        internal Database(Func<Stream> streamFactory, IReadOnlyList<ArrayHeader> headers)
        {
            this.file = new RandomAccessFile(streamFactory);
            this.headers = headers;
        }

        public void Dispose()
        {
            file.Dispose();
        }
    }

    public class DatabaseOpeningBuilder
    {
        private readonly Stream stream;
        private readonly Func<Stream> streamFactory;

        internal DatabaseOpeningBuilder(Stream stream, Func<Stream> streamFactory)
        {
            this.stream = stream;
            this.streamFactory = streamFactory;

            try
            {
                byte[] buffer = new byte[sizeof(long)];
                stream.ReadFully(buffer);
                if(BitConverter.ToInt64(buffer, 0) != 1)
                    throw new InvalidDataException();
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        public DatabaseOpeningBuilder AddArray<T>(ISerializer<T> serializer)
        {
            try
            {
                var header = ReadNextHeader(serializer);
                if (serializer.ElementSize != header.RecordLength)
                    throw new InvalidDataException();
                headers.Add(header);
                return this;
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        private ArrayHeader ReadNextHeader<T>(ISerializer<T> serializer)
        {
            var preHeaderPosition = stream.Position;
            var header = new ArrayHeader();
            var buffer = new byte[sizeof(long)];
            stream.ReadFully(buffer);
            header.RecordCount = BitConverter.ToInt64(buffer, 0);
            stream.ReadFully(buffer);
            header.OverallLength = BitConverter.ToInt64(buffer, 0);
            // TODO: validate header
            var headerBytes = header.AsBytes();
            header.Serializer = serializer;
            header.StartsAt = preHeaderPosition + headerBytes.Length;
            header.EndsAt = header.StartsAt + header.OverallLength;
            return header;
        }

        private List<ArrayHeader> headers = new List<ArrayHeader>();

        public Database Build()
        {
            try
            {
                var db = new Database(streamFactory, headers);
                return db;
            }
            finally
            {
                stream.Dispose();
            }
        }
    }

    public abstract class DatabaseBuilder
    {
        public abstract DatabaseBuilder AddArray<T>(ISerializer<T> serializer, Func<IEnumerable<T>> elements);

        internal abstract Database Finish();

        public Database Build()
        {
            return Finish();
        }
    }

    internal class DatabaseCreationBuilder : DatabaseBuilder
    {
        private readonly string path;

        private List<Func<Stream, ArrayHeader>> actions = new List<Func<Stream, ArrayHeader>>();

        internal override Database Finish()
        {
            var headers = new List<ArrayHeader>();
            using (var stream = File.OpenWrite(path))
            {
                stream.Write(BitConverter.GetBytes(1L));
                foreach (var action in actions)
                {
                    headers.Add(action(stream));
                }
            }

            return new Database(() => Database.OpenReadonly(path), headers);
        }

        public override DatabaseBuilder AddArray<T>(ISerializer<T> serializer, Func<IEnumerable<T>> elements)
        {
            actions.Add(stream =>
            {
                var headerPosition = stream.Position;
                // write a dummy header
                var dummyHeaderBytes = new ArrayHeader().AsBytes();
                stream.Write(dummyHeaderBytes);
                var elementLength = serializer.ElementSize;
                var buffer = new byte[elementLength];
                long elementCount = 0;
                foreach (var element in elements())
                {
                    serializer.Serialize(element, buffer, 0);
                    stream.Write(buffer);
                    elementCount++;
                }

                var pastEndPosition = stream.Position;
                stream.Seek(headerPosition, SeekOrigin.Begin);
                var arrayHeader = new ArrayHeader
                {
                    OverallLength = elementLength * elementCount,
                    RecordCount = elementCount,
                    StartsAt = headerPosition + dummyHeaderBytes.Length,
                    EndsAt = pastEndPosition,
                    Serializer = serializer
                };
                stream.Write(arrayHeader.AsBytes());
                stream.Seek(pastEndPosition, SeekOrigin.Begin);
                return arrayHeader;
            });

            return this;
        }

        internal DatabaseCreationBuilder(string path)
        {
            this.path = path;
        }
    }

    internal class DatabaseCreationOrOpenBuilder : DatabaseBuilder
    {
        private readonly DatabaseOpeningBuilder builder;

        internal DatabaseCreationOrOpenBuilder(Stream stream, Func<Stream> streamFactory)
        {
            this.builder = new DatabaseOpeningBuilder(stream, streamFactory);
        }

        public override DatabaseBuilder AddArray<T>(ISerializer<T> serializer, Func<IEnumerable<T>> elements)
        {
            builder.AddArray(serializer);
            return this;
        }

        internal override Database Finish()
        {
            return builder.Build();
        }
    }

    internal class RandomAccessFile : IDisposable
    {
        public void ReadAt(long offset, byte[] bytes, int start, int length)
        {
            lock (stream)
            {
                stream.Seek(offset, SeekOrigin.Begin);
                stream.ReadFully(bytes, start, length);
            }
        }

        public Stream CreateStreamAt(long offset)
        {
            var s = streamFactory();
            s.Seek(offset, SeekOrigin.Begin);
            return s;
        }

        private readonly Stream stream;

        private readonly Func<Stream> streamFactory;

        public RandomAccessFile(Func<Stream> streamFactory)
        {
            this.stream = streamFactory();
            this.streamFactory = streamFactory;
        }

        public void Dispose()
        {
            lock (stream)
            {
                stream.Dispose();
            }
        }
    }

    internal abstract class ObjectPool<T> : IDisposable
    {
        private readonly object locker = new object();

        private readonly Queue<T> cache = new Queue<T>();

        private readonly int sizeLimit;

        private bool disposed = false;

        // these three methods should not touch `this`.

        protected abstract T Create();

        protected abstract void Reset(T element);

        protected abstract void Dispose(T element);

        public TResult WithElement<TResult>(Func<T, TResult> execute)
        {
            bool isNewElement = true;
            T element = default(T);
            lock (locker)
            {
                if (disposed)
                    throw new ObjectDisposedException(GetType().FullName);

                if (cache.Count != 0)
                {
                    element = cache.Dequeue();
                    isNewElement = false;
                }
            }

            if (isNewElement)
                element = Create();

            TResult result = execute(element);

            Reset(element);

            lock (locker)
            {
                if (disposed || cache.Count >= sizeLimit)
                {
                    Dispose(element);
                }
                else
                {
                    cache.Enqueue(element);
                }
            }

            return result;
        }

        protected ObjectPool(int sizeLimit, IEnumerable<T> initialContents)
        {
            this.sizeLimit = sizeLimit;
            foreach (var element in initialContents)
            {
                cache.Enqueue(element);
            }
        }

        public void Dispose()
        {
            lock (locker)
            {
                disposed = true;
                foreach(var element in cache)
                {
                    Dispose(element);
                }
                cache.Clear();
            }
        }
    }

    public interface ISerializer<T>
    {
        // must return the same value
        // all the time
        int ElementSize { get; }

        T Deserialize(byte[] sourceBuffer, int sourceBufferOffset);

        void Serialize(T element, byte[] destinationBuffer, int destinationBufferOffset);
    }

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