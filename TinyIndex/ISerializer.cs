namespace TinyIndex
{
    public interface ISerializer<T>
    {
        // must return the same value
        // all the time
        int ElementSize { get; }

        T Deserialize(byte[] sourceBuffer, int sourceBufferOffset);

        void Serialize(T element, byte[] destinationBuffer, int destinationBufferOffset);
    }
}