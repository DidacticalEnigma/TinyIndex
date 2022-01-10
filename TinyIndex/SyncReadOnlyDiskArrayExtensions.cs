using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TinyIndex;

public static class SyncReadOnlyDiskArrayExtensions
{
    public static (T element, long id) BinarySearch<T, TKey>(this ISyncReadOnlyDiskArray<T> @this, TKey lookupKey, Func<T, TKey> selector)
        where T : notnull
        where TKey : IComparable<TKey>
    {
        return @this.BinarySearch(lookupKey, selector, Comparer<TKey>.Default);
    }

    public static (long idStart, long idEndExclusive) EqualRange<T, TKey>(this ISyncReadOnlyDiskArray<T> @this, TKey lookupKey, Func<T, TKey> selector)
        where T : notnull
        where TKey : IComparable<TKey>
    {
        return @this.EqualRange(lookupKey, selector, Comparer<TKey>.Default);
    }
    
    public static Task<(T element, long id)> BinarySearchAsync<T, TKey>(this IAsyncReadOnlyDiskArray<T> @this, TKey lookupKey, Func<T, TKey> selector)
        where T : notnull
        where TKey : IComparable<TKey>
    {
        return @this.BinarySearchAsync(lookupKey, selector, Comparer<TKey>.Default);
    }

    public static Task<(long idStart, long idEndExclusive)> EqualRangeAsync<T, TKey>(this IAsyncReadOnlyDiskArray<T> @this, TKey lookupKey, Func<T, TKey> selector)
        where T : notnull
        where TKey : IComparable<TKey>
    {
        return @this.EqualRangeAsync(lookupKey, selector, Comparer<TKey>.Default);
    }
}