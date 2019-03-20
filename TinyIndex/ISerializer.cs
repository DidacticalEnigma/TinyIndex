using System;

namespace TinyIndex
{
    public interface ISerializer<T>
    {
        T Deserialize(ReadOnlySpan<byte> input);

        // serialize to the destination buffer
        // method should return true if the serialization succeeded,
        // and in this situation, actualSize should be filled with the actual size of the buffer used
        // if the serialization failed because of insufficient destination buffer size,
        // the method should return false. in which case the caller can't assume anything
        // about the content of destinationBuffer[destinationBufferOffset .. destinationBufferOffset + destinationBufferLength]
        // and assume nothing about the contents of actualSize
        // the serialization shall not fail for any other reason
        bool TrySerialize(T element, Span<byte> output, out int actualSize);
    }
}