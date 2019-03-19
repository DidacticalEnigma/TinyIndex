using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
            .With(Serializer.ForCollection(Serializer.ForEnum<FileShare>(), enumerable => enumerable.ToArray()))
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
            serializer.TrySerialize(original, buffer, 0, buffer.Length, out var actualSize);
            var resurrected = serializer.Deserialize(buffer, 0, actualSize);
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
            longSerializer.TrySerialize(original, buffer, 0, buffer.Length, out var actualSize);
            var resurrected = longSerializer.Deserialize(buffer, 0, actualSize);
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
                var success = longSerializer.TrySerialize(original, buffer, 0, i, out var actualSize);
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
            var buffer = new byte[16384];
            var original = "織田　信長";
            var stringSerializer = Serializer.ForStringAsUTF8();
            var expectedByteLength = Encoding.UTF8.GetByteCount(original);
            for (int i = 0; i < expectedByteLength + 3; ++i)
            {
                var success = stringSerializer.TrySerialize(original, buffer, 0, i, out var actualSize);
                if (i < expectedByteLength)
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
    }

    public class ComplexComposite
    {
        public int A { get; set; }

        public string B { get; set; }

        public long C { get; set; }

        public FileShare[] D { get; set; }
    }
}
