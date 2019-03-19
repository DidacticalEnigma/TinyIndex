using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace TinyIndex
{
    public static class Serializer
    {
        private class IntSerializer : IConstSizeSerializer<int>
        {
            public int ElementSize => sizeof(int);

            public int Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
            {
                if (sourceBufferLength != ElementSize)
                    throw new InvalidDataException();
                return BitConverter.ToInt32(sourceBuffer, sourceBufferOffset);
            }

            public bool TrySerialize(int element, byte[] destinationBuffer, int destinationBufferOffset, int destinationBufferLength, out int actualSize)
            {
                if (destinationBufferLength < ElementSize)
                {
                    actualSize = 0;
                    return false;
                }

                var bytes = BitConverter.GetBytes(element);
                Array.Copy(bytes, 0, destinationBuffer, destinationBufferOffset, ElementSize);
                actualSize = ElementSize;
                return true;
            }

            public static readonly IntSerializer Serializer = new IntSerializer();
        }

        public static IConstSizeSerializer<int> ForInt()
        {
            return IntSerializer.Serializer;
        }

        private class LongSerializer : IConstSizeSerializer<long>
        {
            public int ElementSize => sizeof(long);

            public long Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
            {
                if (sourceBufferLength != ElementSize)
                    throw new InvalidDataException();
                return BitConverter.ToInt64(sourceBuffer, sourceBufferOffset);
            }

            public bool TrySerialize(long element, byte[] destinationBuffer, int destinationBufferOffset, int destinationBufferLength, out int actualSize)
            {
                if (destinationBufferLength < ElementSize)
                {
                    actualSize = 0;
                    return false;
                }

                var bytes = BitConverter.GetBytes(element);
                Array.Copy(bytes, 0, destinationBuffer, destinationBufferOffset, ElementSize);
                actualSize = ElementSize;
                return true;
            }

            public static readonly LongSerializer Serializer = new LongSerializer();
        }

        public static IConstSizeSerializer<long> ForLong()
        {
            return LongSerializer.Serializer;
        }

        private class StringSerializer : ISerializer<string>
        {
            public string Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
            {
                return Encoding.UTF8.GetString(sourceBuffer, sourceBufferOffset, sourceBufferLength);
            }

            public unsafe bool TrySerialize(
                string element,
                byte[] destinationBuffer,
                int destinationBufferOffset,
                int destinationBufferLength,
                out int actualSize)
            {
                var encoder = Encoding.UTF8.GetEncoder();
                fixed (byte* outputBase = destinationBuffer)
                fixed (char* input = element)
                {
                    byte* output = outputBase + destinationBufferOffset;
                    try
                    {
                        if (destinationBuffer.Length < destinationBufferLength + destinationBufferOffset)
                        {
                            actualSize = 0;
                            return false;
                        }
                        encoder.Convert(input, element.Length, output, destinationBufferLength, true, out _, out var bytesUsed, out var completed);
                        actualSize = completed
                            ? bytesUsed
                            : 0;
                        return completed;
                    }
                    catch (ArgumentException)
                    {
                        actualSize = 0;
                        return false;
                    }
                }
            }

            public static readonly StringSerializer Serializer = new StringSerializer();
        }

        public static ISerializer<string> ForStringAsUTF8()
        {
            return StringSerializer.Serializer;
        }

        private class MappingConstSizeSerializer<TEnum, TUnderlyingType> : IConstSizeSerializer<TEnum>
            where TEnum : Enum
            where TUnderlyingType : struct
        {
            private readonly IConstSizeSerializer<TUnderlyingType> serializer;
            private readonly Func<TEnum, TUnderlyingType> toFunc;
            private readonly Func<TUnderlyingType, TEnum> fromFunc;

            public MappingConstSizeSerializer(
                IConstSizeSerializer<TUnderlyingType> serializer,
                Func<TEnum, TUnderlyingType> toFunc,
                Func<TUnderlyingType, TEnum> fromFunc)
            {
                this.serializer = serializer;
                this.toFunc = toFunc;
                this.fromFunc = fromFunc;
            }

            public TEnum Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
            {
                return fromFunc(serializer.Deserialize(sourceBuffer, sourceBufferOffset, sourceBufferLength));
            }

            public bool TrySerialize(
                TEnum element,
                byte[] destinationBuffer,
                int destinationBufferOffset,
                int destinationBufferLength,
                out int actualSize)
            {
                return serializer.TrySerialize(
                    toFunc(element),
                    destinationBuffer,
                    destinationBufferOffset,
                    destinationBufferLength,
                    out actualSize);
            }

            public int ElementSize => serializer.ElementSize;
        }

        public static IConstSizeSerializer<TEnum> ForEnum<TEnum>()
            where TEnum : Enum
        {
            var type = Enum.GetUnderlyingType(typeof(TEnum));
            if (type == typeof(int))
            {
                return new MappingConstSizeSerializer<TEnum, int>(ForInt(), x => Convert.ToInt32(x), x => (TEnum)Enum.ToObject(typeof(TEnum), x));
            }
            else if (type == typeof(long))
            {
                return new MappingConstSizeSerializer<TEnum, long>(ForLong(), x => Convert.ToInt64(x), x => (TEnum)Enum.ToObject(typeof(TEnum), x));
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private abstract class TypeErasingSerializer : ISerializer<object>
        {
            public abstract object Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength);

            public abstract bool TrySerialize(
                object element,
                byte[] destinationBuffer,
                int destinationBufferOffset,
                int destinationBufferLength,
                out int actualSize);

            public int ElementSize { get; }

            public bool IsConstSize => ElementSize != -1;

            public TypeErasingSerializer(int size = -1)
            {
                this.ElementSize = size;
            }
        }

        private class TypeErasingSerializer<T> : TypeErasingSerializer
        {
            private readonly ISerializer<T> serializer;

            public TypeErasingSerializer(ISerializer<T> serializer) :
                base((serializer as IConstSizeSerializer<T>)?.ElementSize ?? -1)
            {
                this.serializer = serializer;
            }

            public override object Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
            {
                return serializer.Deserialize(sourceBuffer, sourceBufferOffset, sourceBufferLength);
            }

            public override bool TrySerialize(
                object element,
                byte[] destinationBuffer,
                int destinationBufferOffset,
                int destinationBufferLength,
                out int actualSize)
            {
                return serializer.TrySerialize(
                    (T)element,
                    destinationBuffer,
                    destinationBufferOffset,
                    destinationBufferLength,
                    out actualSize);
            }
        }

        private class CompositeSerializer : ISerializer<object[]>
        {
            public object[] Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
            {
                return Iterator().ToArray();

                IEnumerable<object> Iterator()
                {
                    for (int i = 0; i < serializers.Count; ++i)
                    {
                        int elementLength;
                        if (!serializers[i].IsConstSize)
                        {
                            elementLength = BitConverter.ToInt32(sourceBuffer, sourceBufferOffset);
                            sourceBufferOffset += sizeof(int);
                            sourceBufferLength -= sizeof(int);
                        }
                        else
                        {
                            elementLength = serializers[i].ElementSize;
                        }

                        var element = serializers[i].Deserialize(sourceBuffer, sourceBufferOffset, elementLength);
                        sourceBufferOffset += elementLength;
                        sourceBufferLength -= elementLength;
                        yield return element;
                    }
                }
            }

            public bool TrySerialize(
                object[] element,
                byte[] destinationBuffer,
                int destinationBufferOffset,
                int destinationBufferLength,
                out int actualSize)
            {
                using (var memoryStream = new MemoryStream(
                    destinationBuffer,
                    destinationBufferOffset,
                    destinationBufferLength,
                    writable: true))
                using (var binaryWriter = new BinaryWriter(memoryStream))
                {
                    try
                    {
                        var buffer = new byte[32];
                        for (int i = 0; i < element.Length; ++i)
                        {
                            int actualLength;
                            while (!serializers[i].TrySerialize(element[i], buffer, 0, buffer.Length, out actualLength))
                            {
                                Utility.Reallocate(ref buffer);
                            }
                            if(!serializers[i].IsConstSize)
                                binaryWriter.Write(actualLength);
                            binaryWriter.Write(buffer, 0, actualLength);
                        }
                    }
                    catch (NotSupportedException)
                    {
                        actualSize = 0;
                        return false;
                    }

                    actualSize = (int)memoryStream.Length;
                    return true;
                }
            }

            private readonly IReadOnlyList<TypeErasingSerializer> serializers;

            public CompositeSerializer(
                IReadOnlyList<TypeErasingSerializer> serializers)
            {
                this.serializers = serializers;
            }
        }

        private class CollectionSerializer<TCollection, TElement> : ISerializer<TCollection>
            where TCollection : IReadOnlyCollection<TElement>
        {
            public TCollection Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
            {
                return collectionFactory(Iterator());

                IEnumerable<TElement> Iterator()
                {
                    var length = BitConverter.ToInt32(sourceBuffer, sourceBufferOffset);
                    sourceBufferOffset += sizeof(int);
                    sourceBufferLength -= sizeof(int);
                    for (int i = 0; i < length; ++i)
                    {
                        int elementLength;
                        if (!isConstSize)
                        {
                            elementLength = BitConverter.ToInt32(sourceBuffer, sourceBufferOffset);
                            sourceBufferOffset += sizeof(int);
                            sourceBufferLength -= sizeof(int);
                        }
                        else
                        {
                            elementLength = elementSize;
                        }
                        var element = elementSerializer.Deserialize(sourceBuffer, sourceBufferOffset, elementLength);
                        sourceBufferOffset += elementLength;
                        sourceBufferLength -= elementLength;
                        yield return element;
                    }
                }
            }

            public bool TrySerialize(
                TCollection element,
                byte[] destinationBuffer,
                int destinationBufferOffset,
                int destinationBufferLength,
                out int actualSize)
            {
                using (var memoryStream = new MemoryStream(
                    destinationBuffer,
                    destinationBufferOffset,
                    destinationBufferLength,
                    writable: true))
                using (var binaryWriter = new BinaryWriter(memoryStream))
                {
                    try
                    {
                        binaryWriter.Write(element.Count);
                        var buffer = new byte[isConstSize ? elementSize : 32];
                        foreach (var e in element)
                        {
                            int actualLength;
                            while (!elementSerializer.TrySerialize(e, buffer, 0, buffer.Length, out actualLength))
                            {
                                Utility.Reallocate(ref buffer);
                            }
                            if(!isConstSize)
                                binaryWriter.Write(actualLength);
                            binaryWriter.Write(buffer, 0, actualLength);
                        }
                    }
                    catch (NotSupportedException)
                    {
                        actualSize = 0;
                        return false;
                    }

                    actualSize = (int)memoryStream.Length;
                    return true;
                }
            }

            private readonly Func<IEnumerable<TElement>, TCollection> collectionFactory;
            private readonly ISerializer<TElement> elementSerializer;
            private readonly bool isConstSize;
            private readonly int elementSize;

            public CollectionSerializer(
                ISerializer<TElement> elementSerializer,
                Func<IEnumerable<TElement>, TCollection> collectionFactory)
            {
                this.elementSerializer = elementSerializer;
                if (elementSerializer is IConstSizeSerializer<TElement> constSizeSerializer)
                {
                    isConstSize = true;
                    elementSize = constSizeSerializer.ElementSize;
                }
                else
                {
                    isConstSize = false;
                    elementSize = -1;
                }
                this.collectionFactory = collectionFactory;
            }
        }

        public class CompositeBuilder
        {
            private List<TypeErasingSerializer> serializers = new List<TypeErasingSerializer>();

            public CompositeBuilder With<T>(ISerializer<T> serializer)
            {
                serializers.Add(new TypeErasingSerializer<T>(serializer));
                return this;
            }

            public ISerializer<object[]> Create()
            {
                return new CompositeSerializer(serializers);
            }

            internal CompositeBuilder()
            {

            }
        }

        public static CompositeBuilder ForComposite()
        {
            return new CompositeBuilder();
        }

        public static ISerializer<TCollection> ForCollection<TCollection, TElement>(
            ISerializer<TElement> elementSerializer,
            Func<IEnumerable<TElement>, TCollection> collectionFactory)
            where TCollection : IReadOnlyCollection<TElement>
        {
            return new CollectionSerializer<TCollection, TElement>(elementSerializer, collectionFactory);
        }

        public static ISerializer<IReadOnlyCollection<TElement>> ForReadOnlyCollection<TElement>(
            ISerializer<TElement> elementSerializer)
        {
            return new CollectionSerializer<IReadOnlyCollection<TElement>, TElement>(
                elementSerializer,
                enumerable => new ReadOnlyCollection<TElement>(new List<TElement>(enumerable)));
        }

        public static ISerializer<IReadOnlyList<TElement>> ForReadOnlyList<TElement>(
            ISerializer<TElement> elementSerializer)
        {
            return new CollectionSerializer<IReadOnlyList<TElement>, TElement>(
                elementSerializer,
                enumerable => new ReadOnlyCollection<TElement>(new List<TElement>(enumerable)));
        }

        private class MappingSerializer<TFrom, TTo> : ISerializer<TTo>
        {
            private readonly ISerializer<TFrom> serializer;
            private readonly Func<TFrom, TTo> toFunc;
            private readonly Func<TTo, TFrom> fromFunc;

            public MappingSerializer(ISerializer<TFrom> serializer, Func<TFrom, TTo> toFunc, Func<TTo, TFrom> fromFunc)
            {
                this.serializer = serializer;
                this.toFunc = toFunc;
                this.fromFunc = fromFunc;
            }

            public TTo Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
            {
                return toFunc(serializer.Deserialize(sourceBuffer, sourceBufferOffset, sourceBufferLength));
            }

            public bool TrySerialize(
                TTo element,
                byte[] destinationBuffer,
                int destinationBufferOffset,
                int destinationBufferLength,
                out int actualSize)
            {
                return serializer.TrySerialize(
                    fromFunc(element),
                    destinationBuffer,
                    destinationBufferOffset,
                    destinationBufferLength,
                    out actualSize);
            }
        }

        public static ISerializer<TTo> Mapping<TFrom, TTo>(this ISerializer<TFrom> serializer, Func<TFrom, TTo> toFunc, Func<TTo, TFrom> fromFunc)
        {
            return new MappingSerializer<TFrom,TTo>(serializer, toFunc, fromFunc);
        }
    }
}
