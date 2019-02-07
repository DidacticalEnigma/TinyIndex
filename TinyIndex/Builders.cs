using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace TinyIndex
{
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

        public DatabaseOpeningBuilder AddArray<T>(IConstSizeSerializer<T> serializer)
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
        public abstract DatabaseBuilder AddArray<T>(IConstSizeSerializer<T> serializer, Func<IEnumerable<T>> elements);

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

        public override DatabaseBuilder AddArray<T>(IConstSizeSerializer<T> serializer, Func<IEnumerable<T>> elements)
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
                    serializer.TrySerialize(element, buffer, 0, elementLength);
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

        public override DatabaseBuilder AddArray<T>(IConstSizeSerializer<T> serializer, Func<IEnumerable<T>> elements)
        {
            builder.AddArray(serializer);
            return this;
        }

        internal override Database Finish()
        {
            return builder.Build();
        }
    }
}