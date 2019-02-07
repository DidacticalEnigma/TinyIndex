namespace TinyIndex
{
    public interface IConstSizeSerializer<T> : ISerializer<T>
    {
        // must return the same value
        // all the time
        int ElementSize { get; }
    }
}