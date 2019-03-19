using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace TinyIndex.Tests
{
    [TestFixture]
    class UtilityTest
    {
        private static IReadOnlyList<int> CreateTestData()
        {
            var random = new Random(42);
            var list = new List<int>();
            for (int i = 0; i < 500030; ++i)
            {
                list.Add(random.Next(0, 500000));
            }
            list.Sort();
            return list.AsReadOnly();
        }

        private static readonly IReadOnlyList<int> testData = CreateTestData();

        private static (int b, int e) EqualRange(int key)
        {
            var buf = Array.Empty<byte>();
            (int x, long b, long e) = Utility.EqualRange(
                (long id, ref byte[] buffer) => testData[(int)id],
                testData.Count,
                key,
                _ => _,
                Comparer<int>.Default);
            return ((int) b, (int) e);
        }

        [Test]
        public void EqualRangeTest()
        {
            Assert.AreEqual((2, 4), EqualRange(4));
            ;
            ;
        }
    }
}
