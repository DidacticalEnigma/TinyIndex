using System;
using System.Collections.Generic;

namespace TinyIndex
{
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
}