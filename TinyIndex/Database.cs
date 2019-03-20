using System;
using System.Collections.Generic;
using System.IO;

namespace TinyIndex
{
    // Format description:
    // The meta header for the entire file:
    // - 64-bit little endian version of the database file: only version 1 is supported so far
    // - 16 bytes that constitute the GUID of the file itself. Used for migrations.
    // For each array stored:
    //     The array header:
    //     - 64-bit little endian record count
    //     - 64-bit little endian overall length in bytes
    //     - 64-bit little endian type
    public class Database : IDisposable
    {
        private readonly IReadOnlyList<ArrayHeader> headers;
        private readonly RandomAccessFile file;

        public IReadOnlyDiskArray<T> Get<T>(int collectionNumber)
        {
            var header = headers[collectionNumber];
            switch (header.Type)
            {
                case 1:
                    return new ClusteredReadOnlyDiskArray<T>(header, file, (IConstSizeSerializer<T>) header.Serializer);
                case 2:
                    return new NonClusteredReadOnlyDiskArray<T>(header, file, (ISerializer<T>)header.Serializer);
                default:
                    throw new InvalidDataException();
            }
        }

        public static DatabaseOpeningBuilder Open(string path, Guid versionCheck)
        {
            return new DatabaseOpeningBuilder(OpenReadonly(path), () => OpenReadonly(path), versionCheck);
        }

        public static DatabaseBuilder CreateOrOpen(string path, Guid versionCheck)
        {
            try
            {
                // the created stream is kept for the purposes of
                // avoiding TOCTTOU issues, and closed as soon
                // as possible, in the builder's Build function
                return new DatabaseCreationOrOpenBuilder(OpenReadonly(path), () => OpenReadonly(path), versionCheck);
            }
            catch (FileNotFoundException)
            {
                return Create(path, versionCheck);
            }
            catch (InvalidDataException)
            {
                return Create(path, versionCheck);
            }
        }

        public static DatabaseBuilder Create(string path, Guid versionCheck)
        {
            return new DatabaseCreationBuilder(path, versionCheck);
        }

        internal static FileStream OpenReadonly(string path)
        {
            return new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                4096,
                FileOptions.RandomAccess);
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