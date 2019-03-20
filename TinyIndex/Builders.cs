﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

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
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        public DatabaseOpeningBuilder AddArray<T>(IConstSizeSerializer<T> serializer)
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
        {
            var preHeaderPosition = stream.Position;
            var header = new ArrayHeader();
            var buffer = new byte[sizeof(long)];
            stream.ReadFully(buffer);
            header.RecordCount = BitConverter.ToInt64(buffer, 0);
            stream.ReadFully(buffer);
            header.OverallLength = BitConverter.ToInt64(buffer, 0);
            stream.ReadFully(buffer);
            header.Type = BitConverter.ToInt64(buffer, 0);
            // TODO: validate header
            var headerBytes = header.AsBytes();
            header.Serializer = serializer;
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
            Func<IEnumerable<T>> elements)
        {
            return AddArray(serializer, elements, x => x, null);
        }

        public DatabaseBuilder AddArray<T, TKey>(
            IConstSizeSerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            Func<T, TKey> keySelector)
        {
            return AddArray(serializer, elements, keySelector, Comparer<TKey>.Default);
        }

        public DatabaseBuilder AddArray<T>(
            IConstSizeSerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            IComparer<T> comparer)
        {
            return AddArray(serializer, elements, x => x, comparer);
        }

        public abstract DatabaseBuilder AddArray<T, TKey>(
            IConstSizeSerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            Func<T, TKey> keySelector,
            IComparer<TKey> comparer);

        public DatabaseBuilder AddIndirectArray<T>(
            ISerializer<T> serializer,
            Func<IEnumerable<T>> elements)
        {
            return AddIndirectArray(serializer, elements, x => x, null);
        }

        public DatabaseBuilder AddIndirectArray<T>(
            ISerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            IComparer<T> comparer)
        {
            return AddIndirectArray(serializer, elements, x => x, comparer);
        }

        public DatabaseBuilder AddIndirectArray<T, TKey>(
            ISerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            Func<T, TKey> keySelector)
        {
            return AddIndirectArray(serializer, elements, keySelector, Comparer<TKey>.Default);
        }

        public abstract DatabaseBuilder AddIndirectArray<T, TKey>(
            ISerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey> comparer);

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

        private List<Func<Stream, byte[], ArrayHeader>> actions = new List<Func<Stream, byte[], ArrayHeader>>();

        public override DatabaseBuilder AddIndirectArray<T, TKey>(
            ISerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey> comparer)
        {
            actions.Add((stream, buffer) =>
            {
                var headerPosition = stream.Position;
                // write a dummy header
                var dummyHeaderBytes = new ArrayHeader().AsBytes();
                stream.Write(dummyHeaderBytes);

                var pointersArrayOffsetPosition = stream.Position;
                stream.Write(BitConverter.GetBytes(0L));

                var dataStartPosition = stream.Position;
                long elementCount = 0;
                // TODO: less lazy way
                var offsetList = new List<long>();
                var elementsEnumerable = elements();
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
                var arrayHeader = new ArrayHeader
                {
                    OverallLength = pastEndPosition - pointersArrayOffsetPosition,
                    RecordCount = elementCount,
                    StartsAt = pointersArrayOffsetPosition,
                    EndsAt = pastEndPosition,
                    Type = 2,
                    Serializer = serializer
                };
                stream.Write(arrayHeader.AsBytes());
                stream.Write(BitConverter.GetBytes(pointerArrayPosition - pointersArrayOffsetPosition));

                stream.Seek(pointerArrayPosition, SeekOrigin.Begin);
                foreach (var off in offsetList)
                {
                    stream.Write(BitConverter.GetBytes(off));
                }
                stream.Seek(pastEndPosition, SeekOrigin.Begin);
                return arrayHeader;
            });

            return this;
        }

        internal override Database Finish()
        {
            var buffer = new byte[8192];
            var headers = new List<ArrayHeader>();
            using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                stream.Write(BitConverter.GetBytes(0L));
                stream.Write(versionCheck.ToByteArray());
                foreach (var action in actions)
                {
                    headers.Add(action(stream, buffer));
                }

                stream.Seek(0, SeekOrigin.Begin);
                stream.Write(BitConverter.GetBytes(1L));
            }

            return new Database(() => Database.OpenReadonly(path), headers);
        }

        public override DatabaseBuilder AddArray<T, TKey>(
            IConstSizeSerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey> comparer)
        {
            actions.Add((stream, buffer) =>
            {
                var headerPosition = stream.Position;
                // write a dummy header
                var dummyHeaderBytes = new ArrayHeader().AsBytes();
                stream.Write(dummyHeaderBytes);
                var elementLength = serializer.ElementSize;
                Utility.EnsureArrayOfMinimalSize(ref buffer, elementLength);
                long elementCount = 0;
                var elementsEnumerable = elements();
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
                var arrayHeader = new ArrayHeader
                {
                    OverallLength = elementLength * elementCount,
                    RecordCount = elementCount,
                    StartsAt = headerPosition + dummyHeaderBytes.Length,
                    EndsAt = pastEndPosition,
                    Type = 1,
                    Serializer = serializer
                };
                stream.Write(arrayHeader.AsBytes());
                stream.Seek(pastEndPosition, SeekOrigin.Begin);
                return arrayHeader;
            });

            return this;
        }

        internal DatabaseCreationBuilder(string path, Guid versionCheck)
        {
            this.path = path;
            this.versionCheck = versionCheck;
        }
    }

    internal class DatabaseCreationOrOpenBuilder : DatabaseBuilder
    {
        private readonly DatabaseOpeningBuilder builder;

        internal DatabaseCreationOrOpenBuilder(Stream stream, Func<Stream> streamFactory, Guid versionCheck)
        {
            this.builder = new DatabaseOpeningBuilder(stream, streamFactory, versionCheck);
        }

        public override DatabaseBuilder AddArray<T, TKey>(
            IConstSizeSerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey> comparer)
        {
            builder.AddArray(serializer);
            return this;
        }

        public override DatabaseBuilder AddIndirectArray<T, TKey>(
            ISerializer<T> serializer,
            Func<IEnumerable<T>> elements,
            Func<T, TKey> selector,
            IComparer<TKey> comparer)
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