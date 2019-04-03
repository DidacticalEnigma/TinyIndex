using System;
using System.IO;

namespace TinyIndex
{
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

        internal void Reopen()
        {
            lock (stream)
            {
                stream.Dispose();
                stream = streamFactory();
            }
        }

        private Stream stream;

        private readonly Func<Stream> streamFactory;

        public RandomAccessFile(Func<Stream> streamFactory)
        {
            stream = streamFactory();
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
}