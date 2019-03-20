using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace TinyIndex.Tests
{
    [TestFixture]
    class LayoutTest
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
        public void Test()
        {
            var original = new ComplexComposite()
            {
                A = 42,
                B = "John Rambo",
                C = -1,
                D = new FileShare[] { FileShare.Delete, FileShare.Read }
            };
            // For the purposes of this test assume a buffer large enough
            var buffer = new byte[16384];
            serializer.TrySerialize(original, buffer.AsSpan(), out var actualSize);
            Assert.AreEqual(
                "2A0000000A0000004A6F686E2052616D626FFFFFFFFFFFFFFFFF0C000000020000000400000001000000",
                ByteArrayToString(buffer.Take(actualSize).ToArray()));

        }

        private static string ByteArrayToString(byte[] ba)
        {
            return BitConverter.ToString(ba).Replace("-", "");
        }
    }
}
