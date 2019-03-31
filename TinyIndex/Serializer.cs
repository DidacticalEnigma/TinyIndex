using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace TinyIndex
{
    public static class Serializer
    {
        private class IntSerializer : IConstSizeSerializer<int>
        {
            public int ElementSize => sizeof(int);

            public int Deserialize(ReadOnlySpan<byte> input)
            {
                if (input.Length < ElementSize)
                    throw new InvalidDataException();
                return BitConverter.ToInt32(input.Slice(0, 4).ToArray(), 0);
            }

            public bool TrySerialize(int element, Span<byte> output, out int actualSize)
            {
                var bytes = BitConverter.GetBytes(element);
                if (bytes.AsSpan().TryCopyTo(output))
                {
                    actualSize = ElementSize;
                    return true;
                }

                actualSize = 0;
                return false;
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

            public long Deserialize(ReadOnlySpan<byte> input)
            {
                if (input.Length < ElementSize)
                    throw new InvalidDataException();
                return BitConverter.ToInt64(input.Slice(0, 8).ToArray(), 0);
            }

            public bool TrySerialize(long element, Span<byte> output, out int actualSize)
            {
                var bytes = BitConverter.GetBytes(element);
                if (bytes.AsSpan().TryCopyTo(output))
                {
                    actualSize = ElementSize;
                    return true;
                }

                actualSize = 0;
                return false;
            }

            public static readonly LongSerializer Serializer = new LongSerializer();
        }

        public static IConstSizeSerializer<long> ForLong()
        {
            return LongSerializer.Serializer;
        }

        private class StringSerializer : ISerializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> input)
            {
                return Encoding.UTF8.GetString(input.ToArray());
            }

            public unsafe bool TrySerialize(
                string element,
                Span<byte> output,
                out int actualSize)
            {
                var encoder = Encoding.UTF8.GetEncoder();
                fixed (byte* o = output)
                fixed (char* input = element)
                {
                    try
                    {
                        encoder.Convert(input, element.Length, o, output.Length, true, out _, out var bytesUsed, out var completed);
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

            public TEnum Deserialize(ReadOnlySpan<byte> input)
            {
                return fromFunc(serializer.Deserialize(input));
            }

            public bool TrySerialize(TEnum element, Span<byte> output, out int actualSize)
            {
                return serializer.TrySerialize(
                    toFunc(element),
                    output,
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
            public abstract object Deserialize(ReadOnlySpan<byte> input);

            public abstract bool TrySerialize(object element, Span<byte> output, out int actualSize);

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

            public override object Deserialize(ReadOnlySpan<byte> input)
            {
                return serializer.Deserialize(input);
            }

            public override bool TrySerialize(
                object element,
                Span<byte> output,
                out int actualSize)
            {
                return serializer.TrySerialize(
                    (T)element,
                    output,
                    out actualSize);
            }
        }

        private class CompositeSerializer : ISerializer<object[]>
        {
            public object[] Deserialize(ReadOnlySpan<byte> input)
            {
                var list = new List<object>();
                for (int i = 0; i < serializers.Count; ++i)
                {
                    int elementLength;
                    if (!serializers[i].IsConstSize)
                    {
                        elementLength = BitConverter.ToInt32(input.Slice(0, sizeof(int)).ToArray(), 0);
                        input = input.Slice(sizeof(int));
                    }
                    else
                    {
                        elementLength = serializers[i].ElementSize;
                    }

                    var element = serializers[i].Deserialize(input.Slice(0, elementLength));
                    input = input.Slice(elementLength);
                    list.Add(element);
                }

                return list.ToArray();
            }

            public bool TrySerialize(
                object[] element,
                Span<byte> output,
                out int actualSize)
            {
                int overallSize = 0;
                for (int i = 0; i < element.Length; ++i)
                {
                    var start = serializers[i].IsConstSize ? 0 : sizeof(int);
                    var outputLengthBytes = output.Slice(0, Math.Min(start, output.Length));
                    if (serializers[i].TrySerialize(element[i], output.Slice(start), out var actualLength))
                    {
                        var lengthBytes = BitConverter.GetBytes(actualLength);
                        lengthBytes.AsSpan().Slice(0, start).TryCopyTo(outputLengthBytes);
                        output = output.Slice(start + actualLength);
                        overallSize += start + actualLength;
                    }
                    else
                    {
                        actualSize = 0;
                        return false;
                    }
                }

                actualSize = overallSize;
                return true;
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
            public TCollection Deserialize(ReadOnlySpan<byte> input)
            {
                var list = new List<TElement>();
                var length = BitConverter.ToInt32(input.Slice(0, sizeof(int)).ToArray(), 0);
                input = input.Slice(sizeof(int));
                for (int i = 0; i < length; ++i)
                {
                    int elementLength;
                    if (!isConstSize)
                    {
                        elementLength = BitConverter.ToInt32(input.Slice(0, sizeof(int)).ToArray(), 0);
                        input = input.Slice(sizeof(int));
                    }
                    else
                    {
                        elementLength = elementSize;
                    }
                    var element = elementSerializer.Deserialize(input.Slice(0, elementLength));
                    input = input.Slice(elementLength);
                    list.Add(element);
                }

                return collectionFactory(list);
            }

            public bool TrySerialize(
                TCollection element,
                Span<byte> output,
                out int actualSize)
            {
                int overallSize = 0;
                {
                    var lengthBytes = BitConverter.GetBytes(element.Count);
                    if (lengthBytes.AsSpan().TryCopyTo(output))
                    {
                        output = output.Slice(sizeof(int));
                        overallSize += sizeof(int);
                    }
                    else
                    {
                        actualSize = 0;
                        return false;
                    }
                }

                foreach (var e in element)
                {
                    var start = isConstSize ? 0 : sizeof(int);
                    var outputLengthBytes = output.Slice(0, Math.Min(start, output.Length));
                    if (elementSerializer.TrySerialize(e, output.Slice(start), out var actualLength))
                    {
                        var lengthBytes = BitConverter.GetBytes(actualLength);
                        lengthBytes.AsSpan().TryCopyTo(outputLengthBytes);
                        output = output.Slice(start + actualLength);
                        overallSize += start + actualLength;
                    }
                    else
                    {
                        actualSize = 0;
                        return false;
                    }
                }

                actualSize = overallSize;
                return true;
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

        public static ISerializer<(T1, T2)> ForTuple<T1, T2>(ISerializer<T1> s1, ISerializer<T2> s2)
        {
            return ForComposite()
                .With(s1)
                .With(s2)
                .Create()
                .Mapping(
                    raw => ((T1)raw[0], (T2)raw[1]),
                    obj => new object[] { obj.Item1, obj.Item2 });
        }

        public static ISerializer<(T1, T2, T3)> ForTuple<T1, T2, T3>(ISerializer<T1> s1, ISerializer<T2> s2, ISerializer<T3> s3)
        {
            return ForComposite()
                .With(s1)
                .With(s2)
                .With(s3)
                .Create()
                .Mapping(
                    raw => ((T1)raw[0], (T2)raw[1], (T3)raw[2]),
                    obj => new object[] { obj.Item1, obj.Item2, obj.Item3 });
        }

        public static ISerializer<(T1, T2, T3, T4)> ForTuple<T1, T2, T3, T4>(ISerializer<T1> s1, ISerializer<T2> s2, ISerializer<T3> s3, ISerializer<T4> s4)
        {
            return ForComposite()
                .With(s1)
                .With(s2)
                .With(s3)
                .With(s4)
                .Create()
                .Mapping(
                    raw => ((T1)raw[0], (T2)raw[1], (T3)raw[2], (T4)raw[3]),
                    obj => new object[] { obj.Item1, obj.Item2, obj.Item3, obj.Item4 });
        }

        public static ISerializer<TCollection> ForCollection<TCollection, TElement>(
            ISerializer<TElement> elementSerializer,
            Func<IEnumerable<TElement>, TCollection> collectionFactory)
            where TCollection : IReadOnlyCollection<TElement>
        {
            return new CollectionSerializer<TCollection, TElement>(elementSerializer, collectionFactory);
        }

        public static ISerializer<TElement[]> ForArray<TElement>(ISerializer<TElement> elementSerializer)
        {
            return new CollectionSerializer<TElement[], TElement>(
                elementSerializer,
                enumerable => enumerable.ToArray());
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

            public TTo Deserialize(ReadOnlySpan<byte> input)
            {
                return toFunc(serializer.Deserialize(input));
            }

            public bool TrySerialize(
                TTo element,
                Span<byte> output,
                out int actualSize)
            {
                return serializer.TrySerialize(
                    fromFunc(element),
                    output,
                    out actualSize);
            }
        }

        public static ISerializer<TTo> Mapping<TFrom, TTo>(this ISerializer<TFrom> serializer, Func<TFrom, TTo> toFunc, Func<TTo, TFrom> fromFunc)
        {
            return new MappingSerializer<TFrom, TTo>(serializer, toFunc, fromFunc);
        }


        private class DotNetBinarySerializer<T> : ISerializer<T>
        {
            private readonly IFormatter formatter;

            public T Deserialize(ReadOnlySpan<byte> input)
            {
                using (var memoryStream = new MemoryStream(input.ToArray()))
                {
                    return (T)formatter.Deserialize(memoryStream);
                }
            }

            public bool TrySerialize(
                T element,
                Span<byte> output,
                out int actualSize)
            {
                using (var memoryStream = new MemoryStream(output.Length))
                {
                    try
                    {
                        formatter.Serialize(memoryStream, element);
                        var s = (int)memoryStream.Position;
                        if (memoryStream.GetBuffer().AsSpan().Slice(0, s).TryCopyTo(output))
                        {
                            actualSize = s;
                            return true;
                        }

                        actualSize = 0;
                        return false;
                    }
                    catch (NotSupportedException)
                    {
                        actualSize = 0;
                        return false;
                    }
                }
            }

            public DotNetBinarySerializer(IFormatter formatter)
            {
                this.formatter = formatter;
            }
        }

        // uses the .NET built-in serialization API
        // all caveats described in https://docs.microsoft.com/en-us/dotnet/standard/serialization/binary-serialization
        // apply, in particular the warning to not use this for deserializing untrusted data
        public static ISerializer<T> DotNetBinary<T>(IFormatter formatter)
        {
            return new DotNetBinarySerializer<T>(formatter);
        }

        public static ISerializer<KeyValuePair<TKey, TValue>> ForKeyValuePair<TKey, TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            return ForComposite()
                .With(keySerializer)
                .With(valueSerializer)
                .Create()
                .Mapping(
                    raw => new KeyValuePair<TKey, TValue>((TKey)raw[0], (TValue)raw[1]),
                    obj => new object[] { obj.Key, obj.Value });
        }
    }
}
