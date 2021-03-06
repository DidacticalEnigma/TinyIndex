﻿using System;
using System.Collections.Generic;

namespace TinyIndex
{
    public interface IReadOnlyDiskArray<T>
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

    public static class ReadOnlyDiskArrayExtensions
    {
        public static (T element, long id) BinarySearch<T, TKey>(this IReadOnlyDiskArray<T> @this, TKey lookupKey, Func<T, TKey> selector)
            where TKey : IComparable<TKey>
        {
            return @this.BinarySearch(lookupKey, selector, Comparer<TKey>.Default);
        }

        public static (long idStart, long idEndExclusive) EqualRange<T, TKey>(this IReadOnlyDiskArray<T> @this, TKey lookupKey, Func<T, TKey> selector)
            where TKey : IComparable<TKey>
        {
            return @this.EqualRange(lookupKey, selector, Comparer<TKey>.Default);
        }
    }
}