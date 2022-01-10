using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TinyIndex;

public interface IAsyncReadOnlyDiskArray<T>
    where T : notnull
{
    Task<T> GetAsync(long id);

    Task<(T element, long id)> BinarySearchAsync<TKey>(
        TKey lookupKey,
        Func<T, TKey> selector,
        IComparer<TKey> comparer);

    Task<(long idStart, long idEndExclusive)> EqualRangeAsync<TKey>(
        TKey lookupKey,
        Func<T, TKey> selector,
        IComparer<TKey> comparer);

    IAsyncEnumerable<T> GetIdRangeAsync(long idStart, long idEnd);

    IAsyncEnumerable<T> LinearScanAsync();

    ValueTask<long> GetCountAsync();
}