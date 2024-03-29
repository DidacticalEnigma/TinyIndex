﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace TinyIndex.Tests
{
    [TestFixture]
    public class Basic
    {
        [Test]
        public void Test1()
        {
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            using (var database = Database.CreateOrOpen(path, Guid.Empty).AddArray(new IntSerializer(), db => new[] { 1, 2, 3 }).Build())
            {
                var intArr = database.Get<int>(0);
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
            var database = Database.CreateOrOpen(path, Guid.Empty)
                .AddArray(new CompoundTypeSerializer(), db => actual)
                .AddArray(new IntSerializer(), db => new[] { 1, 2, 3, 4, 5 })
                .Build();
            using (database)
            {
                var compArr = database.Get<CompoundType>(0);
                var intArr = database.Get<int>(1);
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
            var database = Database.CreateOrOpen(path, Guid.Empty)
                .AddIndirectArray(new StringSerializer(), db => actual)
                .Build();
            using (database)
            {
                var compArr = database.Get<string>(0);
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
            var database = Database.CreateOrOpen(path, Guid.Empty)
                .AddArray(new IntSerializer(), db => list, Comparer<int>.Default)
                .Build();
            using (database)
            {
                var intArr = database.Get<int>(0);
                Assert.AreEqual(0, intArr.BinarySearch(1739, x => x).id);
                Assert.AreEqual(23, intArr.BinarySearch(46639, x => x).id);
                Assert.AreEqual(22, intArr.BinarySearch(43700, x => x).id);
                Assert.AreEqual(list.Count - 1, intArr.BinarySearch(999998544, x => x).id);
                CollectionAssert.AreEqual(intArr.LinearScan(), list.OrderBy(x => x));
            }
            File.Delete(path);
        }

        [Test]
        public void TestBounds()
        {
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            var random = new Random(42);
            var list = new List<string>();
            for (int i = 0; i < 500; ++i)
            {
                list.Add(random.Next(0, 1000000000).ToString("D30"));
            }

            list.Sort();
            var database = Database.CreateOrOpen(path, Guid.Empty)
                .AddIndirectArray(Serializer.ForStringAsUtf8(), db => list, x => x)
                .Build();
            using (database)
            {
                var intArr = database.Get<string>(0);
                Assert.AreEqual((499, 499), intArr.EqualRange("000000000000000000000994449562", x => x));
                CollectionAssert.AreEqual(intArr.LinearScan(), list.OrderBy(x => x));
            }
            File.Delete(path);
        }

        [Test]
        public void ComplexTest()
        {
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            var random = new Random(42);
            var list = new List<int>();
            for (int i = 0; i < 1; ++i)
            {
                list.Add(-1);
            }

            {
                var database = Database.CreateOrOpen(path, Guid.Empty)
                    .AddIndirectArray(new IntSerializer(), db => list, Comparer<int>.Default)
                    .AddIndirectArray(new CompoundTypeSerializer(),
                        db => new[] {new CompoundType() {A = 42, B = "lowewesdfsfd", C = 69}})
                    .AddIndirectArray(new IntSerializer(), db => list, Comparer<int>.Default)
                    .AddIndirectArray(new CompoundTypeSerializer(),
                        db => new[] {new CompoundType() {A = 42, B = "lowewesdfsfd", C = 69}})
                    .Build();
                using (database)
                {
                    var intArr = database.Get<int>(2);
                    CollectionAssert.AreEqual(intArr.LinearScan(), list.OrderBy(x => x));
                }
            }
            {
                var database = Database.CreateOrOpen(path, Guid.Empty)
                    .AddIndirectArray(new IntSerializer(), db => list, Comparer<int>.Default)
                    .AddIndirectArray(new CompoundTypeSerializer(),
                        db => new[] {new CompoundType() {A = 42, B = "lowewesdfsfd", C = 69}})
                    .AddIndirectArray(new IntSerializer(), db => list, Comparer<int>.Default)
                    .AddIndirectArray(new CompoundTypeSerializer(),
                        db => new[] {new CompoundType() {A = 42, B = "lowewesdfsfd", C = 69}})
                    .Build();
                using (database)
                {
                    var intArr = database.Get<int>(2);
                    CollectionAssert.AreEqual(intArr.LinearScan(), list.OrderBy(x => x));
                }
            }
            File.Delete(path);
        }


        [Test]
        public void AccessingEarlierArrays()
        {
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            var random = new Random(42);
            var list = new List<string>();
            for (int i = 0; i < 500; ++i)
            {
                list.Add(random.Next(0, 1000000000).ToString());
            }

            list.Sort();
            var database = Database.CreateOrOpen(path, Guid.Empty)
                .AddIndirectArray(Serializer.ForStringAsUtf8(), db => list, x => x)
                .AddIndirectArray(Serializer.ForInt(), db => db.Get<string>(0).LinearScan().Select(x => int.Parse(x)))
                .Build();
            using (database)
            {
                var stringArray = database.Get<string>(0);
                var intArray = database.Get<int>(1);
                Assert.AreEqual("827125636", stringArray[400]);
                Assert.AreEqual(827125636, intArray[400]);
            }
            File.Delete(path);
        }

        [Test]
        public void VersioningTest()
        {
            var a = Guid.NewGuid();
            var b = Guid.NewGuid();
            var path = Path.Combine(Path.GetTempPath(), @"asdf.db");
            File.Delete(path);
            using (var database = Database.CreateOrOpen(path, Guid.NewGuid()).AddArray(new IntSerializer(), db => new[] { 4, 5, 6 }).Build())
            {
                var intArr = database.Get<int>(0);
                Assert.AreEqual(intArr[1], 5);
                CollectionAssert.AreEqual(intArr.LinearScan(), new[] { 4, 5, 6 });
            }
            using (var database = Database.CreateOrOpen(path, a).AddArray(new IntSerializer(), db => new[] { 1, 2, 3 }).Build())
            {
                var intArr = database.Get<int>(0);
                Assert.AreEqual(intArr[1], 2);
                CollectionAssert.AreEqual(intArr.LinearScan(), new[] { 1, 2, 3 });
            }
            using (var database = Database.CreateOrOpen(path, a).AddArray(new IntSerializer(), db => new[] { 1, 2, 3 }).Build())
            {
                var intArr = database.Get<int>(0);
                Assert.AreEqual(intArr[1], 2);
                CollectionAssert.AreEqual(intArr.LinearScan(), new[] { 1, 2, 3 });
            }

            try
            {
                using (var database = Database.CreateOrOpen(path, b)
                    .AddArray(new IntSerializer(), db => new []{42, 43, 44})
                    .AddArray(new IntSerializer(), Sequence).Build())
                {
                    
                }

                IEnumerable<int> Sequence(Database db)
                {
                    yield return 42;
                    throw new Exception();
                }
            }
            catch (Exception)
            {
                // by design of the test
            }

            using (var database = Database.CreateOrOpen(path, b).AddArray(new IntSerializer(), db => new[] { 1, 2, 3 }).Build())
            {
                var intArr = database.Get<int>(0);
                Assert.AreEqual(2, intArr[1]);
                CollectionAssert.AreEqual(new[] { 1, 2, 3 }, intArr.LinearScan());
            }
            using (var database = Database.Open(path, b).AddArray(new IntSerializer()).Build())
            {
                var intArr = database.Get<int>(0);
                Assert.AreEqual(2, intArr[1]);
                CollectionAssert.AreEqual(new[] { 1, 2, 3 }, intArr.LinearScan());
            }
            File.Delete(path);
        }
    }

    public class StringSerializer : ISerializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> input)
        {
            var buff = input.ToArray();
            using (var memoryStream = new MemoryStream(buff))
            using (var binaryReader = new BinaryReader(memoryStream))
            {
                return binaryReader.ReadString();
            }
        }

        public bool TrySerialize(string element, Span<byte> output, out int actualSize)
        {
            using (var memoryStream = new MemoryStream())
            using (var binaryWriter = new BinaryWriter(memoryStream))
            {
                binaryWriter.Write(element);
                actualSize = (int)memoryStream.Length;
                if (memoryStream.Length <= output.Length)
                {
                    var source = memoryStream.ToArray();
                    return source.AsSpan().TryCopyTo(output);
                }

                return false;
            }
        }
    }

    public class CompoundType : IEquatable<CompoundType>
    {
        public bool Equals(CompoundType? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return A == other.A && string.Equals(B, other.B) && C == other.C;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((CompoundType)obj);
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

        public string? B { get; set; }

        public long C { get; set; }
    }

    public class CompoundTypeSerializer : IConstSizeSerializer<CompoundType>
    {
        public int ElementSize => 4 + 8 + 32;
        public CompoundType Deserialize(ReadOnlySpan<byte> input)
        {
            if (input.Length < ElementSize)
            {
                throw new InvalidDataException();
            }

            var buff = new byte[ElementSize];
            input.Slice(0, ElementSize).CopyTo(buff.AsSpan());
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

        public bool TrySerialize(CompoundType element, Span<byte> output, out int actualSize)
        {
            var buff = new byte[ElementSize];
            using (var memoryStream = new MemoryStream(buff))
            using (var binaryWriter = new BinaryWriter(memoryStream))
            {
                binaryWriter.Write(element.A);
                binaryWriter.WritePaddedUtf8String(32, element.B!);
                binaryWriter.Write(element.C);
            }

            if (buff.AsSpan().TryCopyTo(output))
            {
                actualSize = ElementSize;
                return true;
            }

            actualSize = 0;
            return false;
        }
    }

    public class IntSerializer : IConstSizeSerializer<int>
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


