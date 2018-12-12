using System;
using System.Collections.Generic;
using System.IO;

namespace TinyIndex
{
    public class Database : IDisposable
    {
        private readonly IReadOnlyList<ArrayHeader> headers;
        private readonly RandomAccessFile file;

        public ClusteredReadOnlyDiskArray<T> Get<T>(int collectionNumber)
        {
            var header = headers[collectionNumber];
            return new ClusteredReadOnlyDiskArray<T>(header, file, (ISerializer<T>)header.Serializer);
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
}