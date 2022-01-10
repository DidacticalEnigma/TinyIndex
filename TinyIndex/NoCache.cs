﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TinyIndex
{
    public class NoCache<TKey, TValue> : ICache<TKey, TValue>
    {
        public int Capacity => 0;

        public TValue Get(TKey key, Func<TValue> valueGenerator)
        {
            return valueGenerator();
        }

        public async Task<TValue> GetAsync(TKey key, Func<Task<TValue>> valueGenerator,
            CancellationToken cancellationToken = default)
        {
            return await valueGenerator();
        }
    }
}
