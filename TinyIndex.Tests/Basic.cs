using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace TinyIndex.Tests
{
    [TestFixture]
    public class Basic
    {
        [Test]
        public void Test()
        {
            using (var db = Database.CreateOrOpen(@"D:\a\asdf.db").AddArray(new IntSerializer(), () => new []{1,2,3}).Build())
            {
                var intArr = db.Get<int>(0);
                Assert.AreEqual(intArr[1], 2);
                CollectionAssert.AreEqual(intArr.LinearScan(), new[]{1,2,3});
            }
        }
    }

    public class IntSerializer : ISerializer<int>
    {
        public int ElementSize => sizeof(int);

        public int Deserialize(byte[] sourceBuffer, int sourceBufferOffset)
        {
            return BitConverter.ToInt32(sourceBuffer, sourceBufferOffset);
        }

        public void Serialize(int element, byte[] destinationBuffer, int destinationBufferOffset)
        {
            var bytes = BitConverter.GetBytes(element);
            Array.Copy(bytes, 0, destinationBuffer, destinationBufferOffset, ElementSize);
        }
    }
}
