using System.Collections.Generic;

namespace TinyIndex
{
    public interface IReadOnlyDiskArray<T>
    {
        T this[long id] { get; }

        (T element, long id) BinarySearch<TKey>(System.Func<T, TKey> selector);
        IEnumerable<T> GetIdRange(long idStart, long idEnd);
        IEnumerable<T> LinearScan();
    }
}