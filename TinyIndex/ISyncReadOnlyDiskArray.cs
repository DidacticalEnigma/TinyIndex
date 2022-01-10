using System;
using System.Collections.Generic;

namespace TinyIndex;

public interface ISyncReadOnlyDiskArray<T>
    where T : notnull
{
    T this[long id] { get; }

    (T element, long id) BinarySearch<TKey>(
        TKey lookupKey,
        Func<T, TKey> selector,
        IComparer<TKey> comparer);

    (long idStart, long idEndExclusive) EqualRange<TKey>(
        TKey lookupKey,
        Func<T, TKey> selector,
        IComparer<TKey> comparer);

    IEnumerable<T> GetIdRange(long idStart, long idEnd);

    IEnumerable<T> LinearScan();

    long Count { get; }
}