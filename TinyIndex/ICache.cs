using System;

namespace TinyIndex
{
    public interface ICache<TKey, TValue>
    {
        /// <summary>
        /// Gets the capacity of the cache.
        /// </summary>
        int Capacity { get; }

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
        TValue Get(TKey key, Func<TValue> valueGenerator);
    }
}