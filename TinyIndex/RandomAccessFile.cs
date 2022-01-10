using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace TinyIndex
{
    internal class RandomAccessFile : IDisposable, IAsyncDisposable
    {
        public async Task ReadAtAsync(long offset, byte[] bytes, int start, int length, CancellationToken cancellationToken = default)
        {
            try
            {
                await locker.WaitAsync(cancellationToken);
                stream.Seek(offset, SeekOrigin.Begin);
                await stream.ReadFullyAsync(bytes, start, length, cancellationToken: cancellationToken);
            }
            finally
            {
                locker.Release();
            }
        }
        
        public void ReadAt(long offset, byte[] bytes, int start, int length)
        {
            try
            {
                locker.Wait();
                stream.Seek(offset, SeekOrigin.Begin);
                stream.ReadFully(bytes, start, length);
            }
            finally
            {
                locker.Release();
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
            try
            {
                locker.Wait();
                stream.Dispose();
                stream = streamFactory();
            }
            finally
            {
                locker.Release();
            }
        }

        private readonly SemaphoreSlim locker = new SemaphoreSlim(1, 1);

        private Stream stream;

        private readonly Func<Stream> streamFactory;

        public RandomAccessFile(Func<Stream> streamFactory)
        {
            stream = streamFactory();
            this.streamFactory = streamFactory;
        }

        public void Dispose()
        {
            try
            {
                locker.Wait();
                stream.Dispose();
            }
            finally
            {
                locker.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await locker.WaitAsync();
                await stream.DisposeAsync();
            }
            finally
            {
                locker.Release();
            }
        }
    }
}