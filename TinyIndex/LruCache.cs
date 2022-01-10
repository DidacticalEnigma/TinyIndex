// https://stackoverflow.com/a/43266537

using System.Threading;
using System.Threading.Tasks;

namespace TinyIndex
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A least-recently-used cache stored like a dictionary.
    /// </summary>
    /// <typeparam name="TKey">
    /// The type of the key to the cached item
    /// </typeparam>
    /// <typeparam name="TValue">
    /// The type of the cached item.
    /// </typeparam>
    /// <remarks>
    /// Derived from https://stackoverflow.com/a/3719378/240845
    /// </remarks>
    public class LruCache<TKey, TValue> : ICache<TKey, TValue>
        where TKey : notnull
        where TValue : notnull
    {
        private SemaphoreSlim locker = new SemaphoreSlim(1, 1);
        
        private readonly Dictionary<TKey, LinkedListNode<LruCacheItem>> cacheMap =
            new Dictionary<TKey, LinkedListNode<LruCacheItem>>();

        private readonly LinkedList<LruCacheItem> lruList =
            new LinkedList<LruCacheItem>();

        private readonly Action<TValue>? dispose;

        /// <summary>
        /// Initializes a new instance of the <see cref="LruCache{TKey, TValue}"/>
        /// class.
        /// </summary>
        /// <param name="capacity">
        /// Maximum number of elements to cache.
        /// </param>
        /// <param name="dispose">
        /// When elements cycle out of the cache, disposes them. May be null.
        /// </param>
        public LruCache(int capacity, Action<TValue>? dispose = null)
        {
            this.Capacity = capacity;
            this.dispose = dispose;
        }

        /// <summary>
        /// Gets the capacity of the cache.
        /// </summary>
        public int Capacity { get; }

        /// <summary>
        /// Looks for a value for the matching <paramref name="key"/>. If not found, 
        /// calls <paramref name="valueGenerator"/> to retrieve the value and add it to
        /// the cache.
        /// </summary>
        /// <param name="key">
        /// The key of the value to look up.
        /// </param>
        /// <param name="valueGenerator">
        /// Generates a value if one isn't found.
        /// </param>
        /// <returns>
        /// The requested value.
        /// </returns>
        public TValue Get(TKey key, Func<TValue> valueGenerator)
        {
            try
            {
                this.locker.Wait();
                TValue value;
                if (this.cacheMap.TryGetValue(key, out var node))
                {
                    value = node.Value.Value;
                    this.lruList.Remove(node);
                    this.lruList.AddLast(node);
                }
                else
                {
                    value = valueGenerator();
                    if (this.cacheMap.Count >= this.Capacity)
                    {
                        this.RemoveFirst();
                    }

                    LruCacheItem cacheItem = new LruCacheItem(key, value);
                    node = new LinkedListNode<LruCacheItem>(cacheItem);
                    this.lruList.AddLast(node);
                    this.cacheMap.Add(key, node);
                }

                return value;
            }
            finally
            {
                this.locker.Release();
            }
        }
        
        public async Task<TValue> GetAsync(TKey key, Func<Task<TValue>> valueGenerator, CancellationToken cancellationToken = default)
        {
            try
            {
                await this.locker.WaitAsync(cancellationToken);
                TValue value;
                if (this.cacheMap.TryGetValue(key, out var node))
                {
                    value = node.Value.Value;
                    this.lruList.Remove(node);
                    this.lruList.AddLast(node);
                }
                else
                {
                    value = await valueGenerator();
                    if (this.cacheMap.Count >= this.Capacity)
                    {
                        this.RemoveFirst();
                    }

                    LruCacheItem cacheItem = new LruCacheItem(key, value);
                    node = new LinkedListNode<LruCacheItem>(cacheItem);
                    this.lruList.AddLast(node);
                    this.cacheMap.Add(key, node);
                }

                return value;
            }
            finally
            {
                this.locker.Release();
            }
        }

        private void RemoveFirst()
        {
            // Remove from LRUPriority
            LinkedListNode<LruCacheItem>? node = this.lruList.First;
            if (node == null)
            {
                return;
            }

            this.lruList.RemoveFirst();

            // Remove from cache
            this.cacheMap.Remove(node.Value.Key);

            // dispose
            this.dispose?.Invoke(node.Value.Value);
        }

        private class LruCacheItem
        {
            public LruCacheItem(TKey k, TValue v)
            {
                this.Key = k;
                this.Value = v;
            }

            public TKey Key { get; }

            public TValue Value { get; }
        }
    }
}