using System;
using System.Collections.Generic;
using System.Text;

namespace TinyIndex
{
    public class NoCache<TKey, TValue> : ICache<TKey, TValue>
    {
        public int Capacity => 0;

        public TValue Get(TKey key, Func<TValue> valueGenerator)
        {
            return valueGenerator();
        }
    }
}
