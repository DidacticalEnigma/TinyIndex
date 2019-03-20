using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace TinyIndex
{
    internal static class Utility
    {
        public static void ReadFully(this Stream stream, byte[] buffer, int offset, int count)
        {
            int numBytesToRead = count;
            int numBytesRead = 0;
            do
            {
                int n = stream.Read(buffer, numBytesRead + offset, numBytesToRead);
                if (n == 0)
                    throw new EndOfStreamException();
                numBytesRead += n;
                numBytesToRead -= n;
            } while (numBytesToRead > 0);
        }

        public static void ReadFully(this Stream stream, byte[] buffer)
        {
            ReadFully(stream, buffer, 0, buffer.Length);
        }

        public static void Write(this Stream stream, byte[] arr)
        {
            stream.Write(arr, 0, arr.Length);
        }

        public static void Append<T>(ref T[] buffer, ref int length, T newElement)
        {
            if (buffer.Length == length)
            {
                Array.Resize(ref buffer, length * 3 / 2 + 1);
            }
            buffer[length] = newElement;
            ++length;
        }

        public static void Reallocate<T>(ref T[] buffer)
        {
            Array.Resize(ref buffer, buffer.Length * 3 / 2 + 1);
        }

        public static void EnsureArrayOfMinimalSize<T>(ref T[] buffer, int minimal)
        {
            if (buffer.Length >= minimal)
                return;
            var newSize = buffer.Length * 3 / 2 + 1;
            if (minimal > newSize)
                newSize = minimal;
            Array.Resize(ref buffer, newSize);
        }

        internal static (long start, long endExclusive) EqualRange<T, TKey>(
            LookupFunc<T> lookup, 
            long len,
            TKey lookupKey,
            Func<T, TKey> selector,
            IComparer<TKey> comparer)
        {
            return (LowerBound(lookup, len, lookupKey, selector, comparer),
                UpperBound(lookup, len, lookupKey, selector, comparer));
        }


        private static long LowerBound<T, TKey>(LookupFunc<T> lookup, long len, TKey lookupKey, Func<T, TKey> selector, IComparer<TKey> comparer)
        {
            long left = 0;
            long right = len;
            var buffer = new byte[sizeof(long)];
            while (left < right)
            {
                var m = GetMidpoint(left, right);
                var record = lookup(m, ref buffer);
                var recordKey = selector(record);
                switch (comparer.Compare(recordKey, lookupKey))
                {
                    case var x when x < 0:
                        left = m + 1;
                        break;
                    default:
                        right = m;
                        break;
                }
            }

            return left;
        }

        private static long UpperBound<T, TKey>(LookupFunc<T> lookup, long len, TKey lookupKey, Func<T, TKey> selector, IComparer<TKey> comparer)
        {
            long left = 0;
            long right = len;
            var buffer = new byte[sizeof(long)];
            while (left < right)
            {
                var m = GetMidpoint(left, right);
                var record = lookup(m, ref buffer);
                var recordKey = selector(record);
                switch (comparer.Compare(recordKey, lookupKey))
                {
                    case var x when x <= 0:
                        left = m + 1;
                        break;
                    default:
                        right = m;
                        break;
                }
            }

            return left;
        }

        public static (T element, long id) BinarySearch<T, TKey>(LookupFunc<T> lookup, long len, TKey lookupKey, Func<T, TKey> selector, IComparer<TKey> comparer)
        {
            long left = 0;
            long right = len - 1;
            var buffer = new byte[sizeof(long)];
            while (left <= right)
            {
                var m = GetMidpoint(left, right);
                var record = lookup(m, ref buffer);
                var recordKey = selector(record);
                switch (comparer.Compare(recordKey, lookupKey))
                {
                    case var x when x < 0:
                        left = m + 1;
                        break;
                    case var x when x > 0:
                        right = m - 1;
                        break;
                    default:
                        return (record, m);
                }
            }

            return (default(T), -1);
        }

        private static long GetMidpoint(long l, long r)
        {
            // To replace if you suffer overflows
            return (l + r) / 2;
        }
    }

    internal delegate T LookupFunc<out T>(long id, ref byte[] buffer);
}