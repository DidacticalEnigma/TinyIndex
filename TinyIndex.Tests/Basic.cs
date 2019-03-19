using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using TinyIndex;
using TinyIndex.Tests;

namespace TinyIndex.Tests
{
    [TestFixture]
    public class Basic
    {
        [Test]
        public void Test()
        {
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            using (var db = Database.CreateOrOpen(path).AddArray(new IntSerializer(), () => new[] { 1, 2, 3 }).Build())
            {
                var intArr = db.Get<int>(0);
                Assert.AreEqual(intArr[1], 2);
                CollectionAssert.AreEqual(intArr.LinearScan(), new[] { 1, 2, 3 });
            }
            File.Delete(path);
        }

        [Test]
        public void Test2()
        {
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            var actual = new[]
            {
                new CompoundType {A = 42, B = "hello", C = 1000},
                new CompoundType {A = 42, B = "John Rambo", C = 1000},
                new CompoundType {A = 1337, B = "John Matrix", C = 1000},
                new CompoundType {A = 1337, B = @"Major Alan ""Dutch"" Schaefer", C = 32},
            };
            var db = Database.CreateOrOpen(path)
                .AddArray(new CompoundTypeSerializer(), () => actual)
                .AddArray(new IntSerializer(), () => new[] {1, 2, 3, 4, 5})
                .Build();
            using (db)
            {
                var compArr = db.Get<CompoundType>(0);
                var intArr = db.Get<int>(1);
                Assert.AreEqual(compArr[2].B, "John Matrix");
                CollectionAssert.AreEqual(compArr.LinearScan(), actual);
                Assert.AreEqual(intArr[1], 2);
                CollectionAssert.AreEqual(intArr.LinearScan(), new[] { 1, 2, 3, 4, 5 });
            }
            File.Delete(path);
        }

        [Test]
        public void Test3()
        {
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            var actual = new[]
            {
                "hello",
                "super long text abcdefghijklmnopqrstuvwxyz",

            };
            var db = Database.CreateOrOpen(path)
                .AddIndirectArray(new StringSerializer(), () => actual)
                .Build();
            using (db)
            {
                var compArr = db.Get<string>(0);
                Assert.AreEqual("hello", compArr[0]);
                CollectionAssert.AreEqual(compArr.LinearScan(), actual);
                Assert.AreEqual("super long text abcdefghijklmnopqrstuvwxyz", compArr[1]);
            }
            File.Delete(path);
        }

        [Test]
        public void Test4()
        {
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            var random = new Random(42);
            var list = new List<int>();
            for (int i = 0; i < 500000; ++i)
            {
                list.Add(random.Next(0, 1000000000));
            }
            var db = Database.CreateOrOpen(path)
                .AddArray(new IntSerializer(), () => list, Comparer<int>.Default)
                .Build();
            using (db)
            {
                var intArr = db.Get<int>(0);
                Assert.AreEqual(0, intArr.BinarySearch(1739, x => x).id);
                Assert.AreEqual(23, intArr.BinarySearch(46639, x => x).id);
                Assert.AreEqual(22, intArr.BinarySearch(43700, x => x).id);
                Assert.AreEqual(list.Count - 1, intArr.BinarySearch(999998544, x => x).id);
                CollectionAssert.AreEqual(intArr.LinearScan(), list.OrderBy(x => x));
            }
            File.Delete(path);
        }
    }

    public class StringSerializer : ISerializer<string>
    {
        public string Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
        {
            var buff = new byte[sourceBufferLength];
            Array.Copy(sourceBuffer, sourceBufferOffset, buff, 0, sourceBufferLength);
            using (var memoryStream = new MemoryStream(buff))
            using (var binaryReader = new BinaryReader(memoryStream))
            {
                return binaryReader.ReadString();
            }
        }

        public bool TrySerialize(string element, byte[] destinationBuffer, int destinationBufferOffset, int destinationBufferLength,
            out int actualSize)
        {
            using (var memoryStream = new MemoryStream())
            using (var binaryWriter = new BinaryWriter(memoryStream))
            {
                binaryWriter.Write(element);
                actualSize = (int)memoryStream.Length;
                if (memoryStream.Length <= destinationBufferLength)
                {
                    var source = memoryStream.ToArray();
                    Array.Copy(source, 0, destinationBuffer, destinationBufferOffset, actualSize);
                    return true;
                }

                return false;
            }
        }
    }

    public class CompoundType : IEquatable<CompoundType>
    {
        public bool Equals(CompoundType other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return A == other.A && string.Equals(B, other.B) && C == other.C;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((CompoundType) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = A;
                hashCode = (hashCode * 397) ^ (B != null ? B.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ C.GetHashCode();
                return hashCode;
            }
        }

        public static bool operator ==(CompoundType left, CompoundType right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(CompoundType left, CompoundType right)
        {
            return !Equals(left, right);
        }

        public int A { get; set; }

        public string B { get; set; }

        public long C { get; set; }
    }

    public class CompoundTypeSerializer : IConstSizeSerializer<CompoundType>
    {
        public int ElementSize => 4 + 8 + 32;
        public CompoundType Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
        {
            if (sourceBufferLength != ElementSize)
            {
                throw new InvalidDataException();
            }

            var buff = new byte[ElementSize];
            Array.Copy(sourceBuffer, sourceBufferOffset, buff, 0, ElementSize);
            var result = new CompoundType();
            using (var memoryStream = new MemoryStream(buff))
            using (var binaryReader = new BinaryReader(memoryStream))
            {
                result.A = binaryReader.ReadInt32();
                result.B = binaryReader.ReadPaddedUtf8String(32);
                result.C = binaryReader.ReadInt64();
            }
            return result;
        }

        public bool TrySerialize(CompoundType element, byte[] destinationBuffer, int destinationBufferOffset, int destinationBufferLength, out int actualSize)
        {
            if (destinationBufferLength != ElementSize)
            {
                actualSize = 0;
                return false;
            }

            var buff = new byte[ElementSize];
            using (var memoryStream = new MemoryStream(buff))
            using (var binaryWriter = new BinaryWriter(memoryStream))
            {
                binaryWriter.Write(element.A);
                binaryWriter.WritePaddedUtf8String(32, element.B);
                binaryWriter.Write(element.C);
            }

            Array.Copy(buff, 0, destinationBuffer, destinationBufferOffset, ElementSize);
            actualSize = ElementSize;
            return true;
        }
    }

    public class IntSerializer : IConstSizeSerializer<int>
    {
        public int ElementSize => sizeof(int);

        public int Deserialize(byte[] sourceBuffer, int sourceBufferOffset, int sourceBufferLength)
        {
            if(sourceBufferLength != ElementSize)
                throw new InvalidDataException();
            return BitConverter.ToInt32(sourceBuffer, sourceBufferOffset);
        }

        public bool TrySerialize(int element, byte[] destinationBuffer, int destinationBufferOffset, int destinationBufferLength, out int actualSize)
        {
            if (destinationBufferLength != ElementSize)
            {
                actualSize = 0;
                return false;
            }

            var bytes = BitConverter.GetBytes(element);
            Array.Copy(bytes, 0, destinationBuffer, destinationBufferOffset, ElementSize);
            actualSize = ElementSize;
            return true;
        }
    }

    public static class BinaryExtensions
    {
        public static string ReadPaddedUtf8String(this BinaryReader reader, int length)
        {
            var bytes = reader.ReadBytes(length);
            var s = Encoding.UTF8.GetString(bytes);
            return s.TrimEnd('\0');
        }

        public static void WritePaddedUtf8String(this BinaryWriter writer, int length, string input)
        {
            var bytes = new byte[length];
            Encoding.UTF8.GetBytes(input, 0, input.Length, bytes, 0);
            // TODO: throw on `input` containing embedded null characters
            writer.Write(bytes);
        }
    }
}


