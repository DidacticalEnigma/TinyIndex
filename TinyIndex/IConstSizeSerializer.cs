namespace TinyIndex
{
    public interface IConstSizeSerializer<T> : ISerializer<T>
        where T : notnull
    {
        // must return the same value
        // all the time
        int ElementSize { get; }
    }
}