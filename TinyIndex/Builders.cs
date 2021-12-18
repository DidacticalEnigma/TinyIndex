using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace TinyIndex
{
    public class DatabaseOpeningBuilder
    {
        private readonly Stream stream;
        private readonly Func<Stream> streamFactory;

        internal DatabaseOpeningBuilder(Stream stream, Func<Stream> streamFactory, Guid versionCheck)
        {
            this.stream = stream;
            this.streamFactory = streamFactory;

            try
            {
                byte[] buffer = new byte[16];
                stream.ReadFully(buffer, 0, sizeof(long));
                if (BitConverter.ToInt64(buffer, 0) != 1)
                    throw new InvalidDataException();
                stream.ReadFully(buffer);
                if (new Guid(buffer) != versionCheck)
                {
                    throw new InvalidDataException();
                }
            }
            catch (EndOfStreamException)
            {
                stream.Dispose();
                throw new InvalidDataException();
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        public DatabaseOpeningBuilder AddArray<T>(IConstSizeSerializer<T> serializer)
            where T : notnull
        {
            try
            {
                var header = ReadNextHeader(serializer);
                if (serializer.ElementSize != ClusteredReadOnlyDiskArray<T>.GetRecordLength(header))
                    throw new InvalidDataException();
                if (header.Type != 1)
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

        public DatabaseOpeningBuilder AddIndirectArray<T>(ISerializer<T> serializer)
            where T : notnull
        {
            try
            {
                var header = ReadNextHeader(serializer);
                if (header.Type != 2)
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
            where T : notnull
        {
            var preHeaderPosition = stream.Position;
            var header = new ArrayHeader(serializer);
            var buffer = new byte[sizeof(long)];
            stream.ReadFully(buffer);
            header.RecordCount = BitConverter.ToInt64(buffer, 0);
            stream.ReadFully(buffer);
            header.OverallLength = BitConverter.ToInt64(buffer, 0);
            stream.ReadFully(buffer);
            header.Type = BitConverter.ToInt64(buffer, 0);
            // TODO: validate header
            var headerBytes = header.AsBytes();
            header.StartsAt = preHeaderPosition + headerBytes.Length;
            header.EndsAt = header.StartsAt + header.OverallLength;
            stream.Seek(header.EndsAt, SeekOrigin.Begin);
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
        public DatabaseBuilder AddArray<T>(
            IConstSizeSerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements)
            where T : notnull
        {
            return AddArray(serializer, elements, x => x, null);
        }

        public DatabaseBuilder AddArray<T, TKey>(
            IConstSizeSerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            Func<T, TKey> keySelector)
            where T : notnull
        {
            return AddArray(serializer, elements, keySelector, Comparer<TKey>.Default);
        }

        public DatabaseBuilder AddArray<T>(
            IConstSizeSerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            IComparer<T> comparer)
            where T : notnull
        {
            return AddArray(serializer, elements, x => x, comparer);
        }

        public abstract DatabaseBuilder AddArray<T, TKey>(
            IConstSizeSerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            Func<T, TKey> keySelector,
            IComparer<TKey>? comparer)
            where T : notnull;

        public DatabaseBuilder AddIndirectArray<T>(
            ISerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements)
            where T : notnull
        {
            return AddIndirectArray(serializer, elements, x => x, null);
        }

        public DatabaseBuilder AddIndirectArray<T>(
            ISerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            IComparer<T> comparer)
            where T : notnull
        {
            return AddIndirectArray(serializer, elements, x => x, comparer);
        }

        public DatabaseBuilder AddIndirectArray<T, TKey>(
            ISerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            Func<T, TKey> keySelector)
            where T : notnull
        {
            return AddIndirectArray(serializer, elements, keySelector, Comparer<TKey>.Default);
        }

        public abstract DatabaseBuilder AddIndirectArray<T, TKey>(ISerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey>? comparer)
            where T : notnull;

        internal abstract Database Finish();

        public Database Build()
        {
            return Finish();
        }
    }

    internal class DatabaseCreationBuilder : DatabaseBuilder
    {
        private readonly string path;
        private readonly Guid versionCheck;
        private byte[] buffer;
        private readonly List<ArrayHeader> headers;
        private readonly Stream stream;
        private readonly Database db;

        public override DatabaseBuilder AddIndirectArray<T, TKey>(
            ISerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey>? comparer)
        {
            try
            {
                var headerPosition = stream.Position;
                // write a dummy header
                var dummyHeaderBytes = new ArrayHeader(serializer).AsBytes();
                stream.Write(dummyHeaderBytes);

                var pointersArrayOffsetPosition = stream.Position;
                stream.Write(BitConverter.GetBytes(0L));

                var dataStartPosition = stream.Position;
                long elementCount = 0;
                // TODO: less lazy way
                var offsetList = new List<long>();
                var elementsEnumerable = elements(db);
                if (comparer != null)
                    elementsEnumerable = elementsEnumerable.OrderBy(selector, comparer);
                foreach (var element in elementsEnumerable)
                {
                    int actualLength;
                    while (!serializer.TrySerialize(element, buffer.AsSpan(), out actualLength))
                    {
                        Utility.Reallocate(ref buffer);
                    }
                    offsetList.Add(stream.Position - dataStartPosition);
                    stream.Write(BitConverter.GetBytes(actualLength));
                    stream.Write(buffer, 0, actualLength);
                    elementCount++;
                }

                var pointerArrayPosition = stream.Position;
                stream.Position += elementCount * sizeof(long);

                var pastEndPosition = stream.Position;
                stream.Seek(headerPosition, SeekOrigin.Begin);
                var arrayHeader = new ArrayHeader(serializer)
                {
                    OverallLength = pastEndPosition - pointersArrayOffsetPosition,
                    RecordCount = elementCount,
                    StartsAt = pointersArrayOffsetPosition,
                    EndsAt = pastEndPosition,
                    Type = 2
                };
                stream.Write(arrayHeader.AsBytes());
                stream.Write(BitConverter.GetBytes(pointerArrayPosition - pointersArrayOffsetPosition));

                stream.Seek(pointerArrayPosition, SeekOrigin.Begin);
                foreach (var off in offsetList)
                {
                    stream.Write(BitConverter.GetBytes(off));
                }
                stream.Seek(pastEndPosition, SeekOrigin.Begin);
                headers.Add(arrayHeader);
            }
            catch
            {
                stream.Dispose();
                db.Dispose();
                throw;
            }
            return this;
        }

        internal override Database Finish()
        {
            try
            {
                stream.Seek(0, SeekOrigin.Begin);
                stream.Write(BitConverter.GetBytes(1L));
                stream.Dispose();
                db.Dispose();
                // prevent delegate from keeping a reference to `this`
                // which would prevent the builder from being garbage collected.
                var p = path;
                return new Database(() => Database.OpenReadonly(p), headers);
            }
            catch
            {
                stream.Dispose();
                db.Dispose();
                throw;
            }
        }

        public override DatabaseBuilder AddArray<T, TKey>(
            IConstSizeSerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey>? comparer)
        {
            try
            {
                var headerPosition = stream.Position;
                // write a dummy header
                var dummyHeaderBytes = new ArrayHeader(serializer).AsBytes();
                stream.Write(dummyHeaderBytes);
                var elementLength = serializer.ElementSize;
                Utility.EnsureArrayOfMinimalSize(ref buffer, elementLength);
                long elementCount = 0;
                var elementsEnumerable = elements(db);
                if (comparer != null)
                    elementsEnumerable = elementsEnumerable.OrderBy(selector, comparer);
                foreach (var element in elementsEnumerable)
                {
                    serializer.TrySerialize(element, buffer.AsSpan(), out _);
                    stream.Write(buffer, 0, elementLength);
                    elementCount++;
                }

                var pastEndPosition = stream.Position;
                stream.Seek(headerPosition, SeekOrigin.Begin);
                var arrayHeader = new ArrayHeader(serializer)
                {
                    OverallLength = elementLength * elementCount,
                    RecordCount = elementCount,
                    StartsAt = headerPosition + dummyHeaderBytes.Length,
                    EndsAt = pastEndPosition,
                    Type = 1
                };
                stream.Write(arrayHeader.AsBytes());
                stream.Seek(pastEndPosition, SeekOrigin.Begin);
                headers.Add(arrayHeader);
            }
            catch
            {
                stream.Dispose();
                db.Dispose();
                throw;
            }

            return this;
        }

        internal DatabaseCreationBuilder(string path, Guid versionCheck)
        {
            this.path = path;
            this.versionCheck = versionCheck;
            buffer = new byte[8192];
            headers = new List<ArrayHeader>();
            stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
            db = new Database(() => Database.Open(FileShare.ReadWrite | FileShare.Delete, path), headers);
            try
            {
                stream.Write(BitConverter.GetBytes(0L));
                stream.Write(versionCheck.ToByteArray());
            }
            catch
            {
                stream.Dispose();
                db.Dispose();
            }
        }
    }

    internal class DatabaseCreationOrOpenBuilder : DatabaseBuilder
    {
        private readonly DatabaseOpeningBuilder builder;

        internal DatabaseCreationOrOpenBuilder(Stream stream, Func<Stream> streamFactory, Guid versionCheck)
        {
            builder = new DatabaseOpeningBuilder(stream, streamFactory, versionCheck);
        }

        public override DatabaseBuilder AddArray<T, TKey>(IConstSizeSerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey>? comparer)
        {
            builder.AddArray(serializer);
            return this;
        }

        public override DatabaseBuilder AddIndirectArray<T, TKey>(ISerializer<T> serializer,
            Func<Database, IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey>? comparer)
        {
            builder.AddIndirectArray(serializer);
            return this;
        }

        internal override Database Finish()
        {
            return builder.Build();
        }
    }
}