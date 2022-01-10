namespace TinyIndex
{
    public interface IReadOnlyDiskArray<T> : ISyncReadOnlyDiskArray<T>, IAsyncReadOnlyDiskArray<T>
        where T : notnull
    {
    }
}