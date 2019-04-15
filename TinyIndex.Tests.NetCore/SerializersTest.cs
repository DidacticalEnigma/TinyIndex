using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using NUnit.Framework;

namespace TinyIndex.Tests
{
    [TestFixture]
    class SerializersTest
    {
        private static readonly ISerializer<ComplexComposite> serializer = Serializer.ForComposite()
            .With(Serializer.ForInt())
            .With(Serializer.ForStringAsUTF8())
            .With(Serializer.ForLong())
            .With(Serializer.ForArray(Serializer.ForEnum<FileShare>()))
            .Create()
            .Mapping(raw => new ComplexComposite()
            {
                A = (int)raw[0],
                B = (string)raw[1],
                C = (long)raw[2],
                D = (FileShare[])raw[3]
            }, obj => new object[]
            {
                obj.A,
                obj.B,
                obj.C,
                obj.D
            });


        [Test]
        public void Test1()
        {
            var original = new ComplexComposite()
            {
                A = 42,
                B = "John Rambo",
                C = -1,
                D = new FileShare[] {FileShare.Delete, FileShare.Read}
            };
            // For the purposes of this test assume a buffer large enough
            var buffer = new byte[16384];
            serializer.TrySerialize(original, buffer.AsSpan(), out var actualSize);
            var resurrected = serializer.Deserialize(buffer.AsSpan().Slice(0, actualSize));
            Assert.AreEqual(original.A, resurrected.A);
            Assert.AreEqual(original.B, resurrected.B);
            Assert.AreEqual(original.C, resurrected.C);
            CollectionAssert.AreEqual(original.D, resurrected.D);
        }

        [Test]
        public void Test2()
        {
            // For the purposes of this test assume a buffer large enough
            var buffer = new byte[16384];
            var longSerializer = Serializer.ForLong();
            long original = -1;
            longSerializer.TrySerialize(original, buffer.AsSpan(), out var actualSize);
            var resurrected = longSerializer.Deserialize(buffer.AsSpan().Slice(0, actualSize));
            Assert.AreEqual(original, resurrected);
        }

        [Test]
        public void LongSerializer()
        {
            var buffer = new byte[16384];
            var original = -1L;
            var longSerializer = Serializer.ForLong();
            for (int i = 0; i < sizeof(long) + 2; ++i)
            {
                var success = longSerializer.TrySerialize(original, buffer.AsSpan().Slice(0, i), out var actualSize);
                if (i < sizeof(long))
                {
                    Assert.AreEqual(false, success);
                    Assert.AreEqual(0, actualSize);
                }
                else
                {
                    Assert.AreEqual(true, success);
                    Assert.AreEqual(sizeof(long), actualSize);
                }
            }
        }

        [Test]
        public void StringSerializer()
        {
            var original = "織田　信長";
            var expectedByteLength = Encoding.UTF8.GetByteCount(original);
            var buffer = new byte[expectedByteLength + 2];
            var stringSerializer = Serializer.ForStringAsUTF8();
            for (int i = 0; i < expectedByteLength + 2; ++i)
            {
                var success = stringSerializer.TrySerialize(original, buffer.AsSpan().Slice(0, i), out var actualSize);
                if (i < expectedByteLength || buffer.Length < i)
                {
                    Assert.AreEqual(false, success);
                    Assert.AreEqual(0, actualSize);
                }
                else
                {
                    Assert.AreEqual(true, success);
                    Assert.AreEqual(expectedByteLength, actualSize);
                }
            }
        }

        [Test]
        public void Binary()
        {
            var original = new ComplexComposite()
            {
                A = 42,
                B = "John Rambo",
                C = -1,
                D = new FileShare[] { FileShare.Delete, FileShare.Read }
            };
            // For the purposes of this test assume a buffer large enough
            var binarySerializer = Serializer.DotNetBinary<ComplexComposite>(new BinaryFormatter());
            var buffer = new byte[16384];
            binarySerializer.TrySerialize(original, buffer.AsSpan(), out var actualSize);
            Assert.IsTrue(actualSize < buffer.Length);
            var resurrected = binarySerializer.Deserialize(buffer.AsSpan().Slice(0, actualSize));
            Assert.AreEqual(original.A, resurrected.A);
            Assert.AreEqual(original.B, resurrected.B);
            Assert.AreEqual(original.C, resurrected.C);
            CollectionAssert.AreEqual(original.D, resurrected.D);
        }
    }

    [Serializable]
    public class ComplexComposite
    {
        public int A { get; set; }

        public string B { get; set; }

        public long C { get; set; }

        public FileShare[] D { get; set; }
    }
}
