using System.Collections.Generic;
using System.Text;
using System.Runtime;
using System.Runtime.Serialization;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.Versioning;
using System.Security;
using System.Threading;
using System.Globalization;
using System.Linq;

namespace sharpeners {

    // This class represents a mutable array of value types and structs. 
    // The VAST majority of this code is based on the StringBuilder implementation from Microsoft,
    // so much that the documentation might still reference strings and characters... :S 
    // The unsafe code was removed to use only managed code, as well as string-oriented methods.
    // 
    // The idea is to take the benefit of StringBuilder for value types, since the arrays created for such
    // types get the very values stored in there, instead of just references. 
    // On very large arrays of big value types (decimal, double, or custom structs), this typically means huge segments of
    // contiguous memory initially in gen1/2, then in the large object heap, which can easily cause OOM exceptions
    // 
    // As for the StringBuilder, the typical usage of the class (Append, Append, ..., then ToString) is well optimized, 
    // which is the point in reusing it there. That means that Append operations are typically fast. 
    // On the other hand, modifications of the instance (like Insert, Replace, Remove) not so much, and frequent 
    // Read operations are clearly expensive. The reason for the good and bad things is rooted in the design, 
    // which is a linked list of StringBuilder, with the important detail that it is "reversed": the head of
    // the list is actually the "end" (ie the last appended segment) of the array, in order to speed up Append operations.
    //
    // This implementation for value types tries to help with the problems related to the original implementation 
    // by conceptually extending the basic linked list to a skip list (see https://en.wikipedia.org/wiki/Skip_list), 
    // to speed up traversal for index-based operations. 
    //
    // The references of farther away "chunks" are kept in a dictionary for every other chunk. Also, higher 
    // order references are added every time the list size doubles. The direct, one-to-one references 
    // are still done with m_ChunkPrevious as originally implemented in StringBuilder, but higher order references 
    // are kept in a dictionary. The dictionaries themselves are kepts as small as possible by using ushort for the keys,
    // which represent the exponent used for the power to base 2
    // Ex: With a StructArrayBuilder of 21 chunks, the references are as below
    //                     16---------------------------------------------------->0 k = 4
    //                     16---------------------------->8---------------------->0 k = 3
    //     20------------->16------------->12------------>8---------->4---------->0 k = 2
    //     20----->18----->16----->14----->12----->10---->8---->6---->4---->2---->0 k = 1
    // 21->20->19->18->17->16->15->14->13->12->11->10->9->8->7->6->5->4->3->2->1->0 
    //
    // The resulting implementation uses the original traversal method for sizes below 400 chunks, 
    // since each traversal operation is in itself extremely fast since is is just a reference change and a simple condition,
    // compared to looking up in the dictionary (although the execution time difference is (in absolute terms) minor). 
    // It switches to skip list traversal when the number of chunks is high, which essentially provides constant 
    // Read execution, even for far-off chunks (like the first values in the mutable array)
    //
    // Insert and Remove operations do not benefit from the implementation. They need full traversal up to the 
    // matching chunks, since the change in the overall length needs to ne propagated down to all chunks. Also, 
    // they theorically degrade the performance of Read operations since the distribution of the rapidly skipable
    // items loses consistency, although I couldn't measure any real effect.
    // 
    // For my needs though, this provides a clear improvement, since I can now re-implement another core object,
    // the MemoryStream, by using a StructArrayBuilder<byte> as the underlying storage without running into
    // the memory problems of having a simple immutable array, and still maintain fast Stream.Write and Stream.Read 
    // operations
    public class StructArrayBuilder<T>  where T : struct    {
        // A StructArrayBuilder is internally represented as a linked list of blocks each of which holds
        // a chunk of the array.   

        //
        //
        //  CLASS VARIABLES
        //
        //
        internal T[] m_ChunkValues;                // The items in this block
        internal StructArrayBuilder<T> m_ChunkPrevious;      // Link to the block logically before this block
        internal int m_ChunkLength;                  // The index in m_ChunkChars that represent the end of the block
        internal int m_ChunkOffset;                  // The logial offset (sum of all items in previous blocks)
        internal int m_MaxCapacity = 0;
        internal int m_ChunkIndex;
        internal SortedDictionary<ushort, StructArrayBuilder<T>> m_ChunkReferences;
        internal bool useSkipLists;
        //
        //
        // STATIC CONSTANTS
        //
        //
        internal const int DefaultCapacity = 16;

        // We want to keep chunk arrays out of large object heap (< 85K bytes ~ 40K chars) to be sure.
        // In StringBuilder this is set to 8000, which is a compromise between less allocation as possible and as fast as possible inserts/replace.
        // In this case the struct can be of any size. In cade of Decimal, it's 16 bytes, and custom structs are likely to be at that size, or bigger
        // so we take a smaller max chunk zise
        internal const int MaxChunkSize = 2000;        
        
        //
        internal const int SkipListUsageMinimumIndex = 400;

        //
        //
        //CONSTRUCTORS
        //
        //

        // Creates a new empty array builder 
        // with the default capacity (16 items).
        public StructArrayBuilder(bool useSkipLists =true)
            : this(DefaultCapacity, useSkipLists) {
        }

        // Create a new empty array builder
        // with the specified capacity.
        public StructArrayBuilder(int capacity, bool useSkipLists =true)
            : this(null, capacity, useSkipLists) {
        }

        // Creates a new array builder from the specified array
        // with the default capacity
        public StructArrayBuilder(T[] values, bool useSkipLists =true)
            : this(values, DefaultCapacity, useSkipLists) {
        }

        // Creates a new array builder from the specified array
        //  with the specified capacity. 
        public StructArrayBuilder(T[] values, int capacity, bool useSkipLists =true)
            : this(values, 0, ((values != null) ? values.Length : 0), capacity, useSkipLists) {
        }

        // Creates a new array builder from the specifed sub array with the specified
        // capacity.  The maximum number of element is set by capacity.
        // 
        
        public StructArrayBuilder(T[] values, int startIndex, int length, int capacity, bool useSkipLists =true) {
            if (capacity<0) {
                throw new ArgumentOutOfRangeException("capacity","Capacity must be positive");
            }
            if (length<0) {
                throw new ArgumentOutOfRangeException("length","Length must be positive");
            }
            if (startIndex<0) {
                throw new ArgumentOutOfRangeException("startIndex", "StartIndex must be positive");
            }

            if (values == null) {
                values = new T[0];
            }
            if (startIndex > values.Length - length) {
                throw new ArgumentOutOfRangeException("length");
            }

            this.useSkipLists = useSkipLists;

            m_MaxCapacity = Int32.MaxValue;
            if (capacity == 0) {
                capacity = DefaultCapacity;
            }

            if (capacity < length)
                capacity = length;

            m_ChunkValues = new T[capacity];
            m_ChunkLength = length;

            Array.Copy(values, m_ChunkValues, length);
        }

        // Creates an empty StructArrayBuilder with a minimum capacity of capacity
        // and a maximum capacity of maxCapacity.
        public StructArrayBuilder(int capacity, int maxCapacity, bool useSkipLists =true) {
            if (capacity>maxCapacity) {
                throw new ArgumentOutOfRangeException("capacity", "Minimum capacity cannot be less than maximum capacity");
            }
            if (maxCapacity<1) {
                throw new ArgumentOutOfRangeException("maxCapacity", "Maximum capacity must be more than 0");
            }
            if (capacity<0) {
                throw new ArgumentOutOfRangeException("capacity", "Minimum capacity must be more than 0");
            }

            if (capacity == 0) {
                capacity = Math.Min(DefaultCapacity, maxCapacity);
            }

            m_MaxCapacity = maxCapacity;
            m_ChunkValues = new T[capacity];
        }

        public int Capacity {
            get { return m_ChunkValues.Length + m_ChunkOffset; }
            set {
                if (value < 0) {
                    throw new ArgumentOutOfRangeException("value", "Capacity must be positive");
                }
                if (value > MaxCapacity) {
                    throw new ArgumentOutOfRangeException("value", "Capacity must be less than maximum capacity ("+MaxCapacity+")");
                }
                if (value < Length) {
                    throw new ArgumentOutOfRangeException("value", "Capacity cannot be less than the current length ("+Length+")");
                }

                if (Capacity != value) {
                    int newLen = value - m_ChunkOffset;
                    T[] newArray = new T[newLen];
                    Array.Copy(m_ChunkValues, newArray, m_ChunkLength);
                    m_ChunkValues = newArray;
                }
            }
        }

        // Read-Only Property 
        public int MaxCapacity {
            get { return m_MaxCapacity; }
        }

        // Ensures that the capacity of this array builder is at least the specified value.  
        // If capacity is greater than the capacity of this array builder, then the capacity
        // is set to capacity; otherwise the capacity is unchanged.
        public int EnsureCapacity(int capacity) {
            if (capacity < 0) {
                throw new ArgumentOutOfRangeException("capacity", "Capcity must be positive");
            }

            if (Capacity < capacity)
                Capacity = capacity;
            return Capacity;
        }

        public long MemSize{ 
            get {
                var tSize = System.Runtime.InteropServices.Marshal.SizeOf(typeof(T));
                var size=0L;
                var chunk = this;
                do{
                    size += tSize * chunk.m_ChunkValues.Length;
                    if(chunk.m_ChunkReferences != null){
                        // key: short=2bytes | value: reference=8bytes
                        size += chunk.m_ChunkReferences.Count * (2 + 8);
                    }
                    chunk = chunk.m_ChunkPrevious;
                }while(chunk != null);
                return size;
            }
        }
        
        public T[] ToArray() {

            if (Length == 0)
                return new T[0];

            var ret = new T[Length];
            var chunk = this;
            do
            {
                if (chunk.m_ChunkLength > 0)
                {
                    T[] sourceArray = chunk.m_ChunkValues;
                    int chunkOffset = chunk.m_ChunkOffset;
                    int chunkLength = chunk.m_ChunkLength;

                    // Check that we will not overrun our boundaries. 
                    if ((uint)(chunkLength + chunkOffset) <= ret.Length && (uint)chunkLength <= (uint)sourceArray.Length)
                    {

                        Array.Copy(sourceArray, 0, ret, chunkOffset, chunkLength);
                    }
                    else
                    {
                        throw new ArgumentOutOfRangeException("chunkLength", "Existing length is greater than allocated");
                    }
                }
                chunk = chunk.m_ChunkPrevious;
            } while (chunk != null);
            
            return ret;
        }


        // Converts a part of this builder to an array
        public T[] ToArray(int startIndex, int length) {
            int currentLength = this.Length;
            if (startIndex < 0)
            {
                throw new ArgumentOutOfRangeException("startIndex", "Start index must be positive");
            }
            if (startIndex > currentLength)
            {
                throw new ArgumentOutOfRangeException("startIndex", "Start index cannot be greater than length");
            }
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException("length", "length must be positive");
            }
            if (startIndex > (currentLength - length))
            {
                throw new ArgumentOutOfRangeException("length", "length value would imply going over the instance Length");
            }

            int sourceEndIndex = startIndex + length;
            var chunk = FindChunkForIndex(sourceEndIndex);

            T[] ret = new T[length];
            int curDestIndex = length;
            while (curDestIndex > 0)
            {
                int chunkEndIndex = sourceEndIndex - chunk.m_ChunkOffset;
                if (chunkEndIndex >= 0)
                {
                    if (chunkEndIndex > chunk.m_ChunkLength)
                        chunkEndIndex = chunk.m_ChunkLength;

                    int countLeft = curDestIndex;
                    int chunkCount = countLeft;
                    int chunkStartIndex = chunkEndIndex - countLeft;
                    if (chunkStartIndex < 0)
                    {
                        chunkCount += chunkStartIndex;
                        chunkStartIndex = 0;
                    }
                    curDestIndex -= chunkCount;

                    if (chunkCount > 0)
                    {
                        // work off of local variables so that they are stable even in the presence of ----s (hackers might do this)
                        T[] sourceArray = chunk.m_ChunkValues;

                        // Check that we will not overrun our boundaries. 
                        if ((uint)(chunkCount + curDestIndex) <= length && (uint)(chunkCount + chunkStartIndex) <= (uint)sourceArray.Length)
                        {

                            Array.Copy(sourceArray, chunkStartIndex, ret, curDestIndex, chunkCount);

                        }
                        else
                        {
                            throw new ArgumentOutOfRangeException("chunkCount", "Existing length is greater than allocated");
                        }
                    }
                }
                chunk = chunk.m_ChunkPrevious;
            }
            return ret;
        }

        // Convenience method for sb.Length=0;
        public StructArrayBuilder<T> Clear() {
            this.Length = 0;
            return this;
        }

        // Sets the length of the String in this buffer.  If length is less than the current
        // instance, the StructArrayBuilder is truncated.  If length is greater than the current 
        // instance, nulls are appended.  The capacity is adjusted to be the same as the length.
        public int Length {
            get {
                return m_ChunkOffset + m_ChunkLength;
            }
            set {
                //If the new length is less than 0 or greater than our Maximum capacity, bail.
                if (value<0) {
                    throw new ArgumentOutOfRangeException("value", "Value must be positive");
                }

                if (value>MaxCapacity) {
                    throw new ArgumentOutOfRangeException("value", "Value cannot be more than the maximum capacity");
                }
                int originalCapacity = Capacity;

                if (value == 0 && m_ChunkPrevious == null)
                {
                    m_ChunkLength = 0;
                    m_ChunkOffset = 0;
                    return;
                }

                int delta = value - Length;
                // if the specified length is greater than the current length
                if (delta > 0)
                {
                    // ... then we add default values for the difference
                    Append(default(T), delta);        
                }
                // if the specified length is less than or equal to the current length
                else
                {
                    var chunk = FindChunkForIndex(value);
                    if (chunk != this)
                    {
                        // we crossed a chunk boundary when reducing the Length, we must replace this middle-chunk with a new
                        // larger chunk to ensure the original capacity is preserved
                        int newLen = originalCapacity - chunk.m_ChunkOffset;
                        T[] newArray = new T[newLen];

                        Array.Copy(chunk.m_ChunkValues, newArray, chunk.m_ChunkLength);
                        
                        m_ChunkValues = newArray;
                        m_ChunkPrevious = chunk.m_ChunkPrevious;                        
                        m_ChunkOffset = chunk.m_ChunkOffset;
                    }
                    m_ChunkLength = value - chunk.m_ChunkOffset;
                }
            }
        }

        [System.Runtime.CompilerServices.IndexerName("Values")]
        public T this[int index] {
            // 

            get {
                var chunk = FindChunkForIndex(index);
                for (; ; )
                {
                    int indexInBlock = index - chunk.m_ChunkOffset;
                    if (indexInBlock >= 0)
                    {
                        if (indexInBlock >= chunk.m_ChunkLength)
                            throw new IndexOutOfRangeException();
                        return chunk.m_ChunkValues[indexInBlock];
                    }
                    chunk = chunk.m_ChunkPrevious;
                    if (chunk == null)
                        throw new IndexOutOfRangeException();
                }
            }
            set {
                var chunk = FindChunkForIndex(index);
                for (; ; )
                {
                    int indexInBlock = index - chunk.m_ChunkOffset;
                    if (indexInBlock >= 0)
                    {
                        if (indexInBlock >= chunk.m_ChunkLength)
                            throw new ArgumentOutOfRangeException("index");
                        chunk.m_ChunkValues[indexInBlock] = value;
                        return;
                    }
                    chunk = chunk.m_ChunkPrevious;
                    if (chunk == null)
                        throw new ArgumentOutOfRangeException("index");
                }
            }
        }

        // Appends an item at the end of this array builder. The capacity is adjusted as needed.
        public StructArrayBuilder<T> Append(T value, int repeatCount) {
            if (repeatCount<0) {
                throw new ArgumentOutOfRangeException("repeatCount", "Repeat count must be positive");
            }

            if (repeatCount==0) {
                return this;
            }
            int idx = m_ChunkLength;
            while (repeatCount > 0)
            {
                if (idx < m_ChunkValues.Length)
                {
                    m_ChunkValues[idx++] = value;
                    --repeatCount;
                }
                else
                {
                    m_ChunkLength = idx;
                    ExpandByABlock(repeatCount);
                    idx = 0;
                }
            }
            m_ChunkLength = idx;
            return this;
        }

        // Appends an array of items at the end of this array builder. The capacity is adjusted as needed. 
        public StructArrayBuilder<T> Append(T[] values, int startIndex, int count) {
            if (startIndex < 0) {
                throw new ArgumentOutOfRangeException("startIndex", "Start index must be positive");
            }
            if (count<0) {
                throw new ArgumentOutOfRangeException("count", "Count must be positive");
            }

            if (values == null) {
                if (startIndex == 0 && count == 0) {
                    return this;
                }
                throw new ArgumentNullException("values");
            }
            if (count > values.Length - startIndex) {
                throw new ArgumentOutOfRangeException("count", "Count cannot be greater than possible interval");
            }

            if (count==0) {
                return this;
            }

            var newIndex = count + m_ChunkLength;
            if(newIndex <= m_ChunkValues.Length)
            {
                Array.Copy(values, startIndex, m_ChunkValues, m_ChunkLength, count);
                m_ChunkLength += count;
            }else
            {
                var firstSegmentLength = m_ChunkValues.Length - m_ChunkLength;
                if(firstSegmentLength > 0){
                    Array.Copy(values, startIndex, m_ChunkValues, m_ChunkLength, firstSegmentLength);
                    m_ChunkLength += firstSegmentLength;
                }
                
                var restLength = count - firstSegmentLength;
                ExpandByABlock(restLength);

                Array.Copy(values, startIndex + firstSegmentLength, m_ChunkValues, 0, restLength);
                m_ChunkLength = restLength;
            }

            return this;
        }


        // Appends a copy of this string at the end of this array builder.
        
        public StructArrayBuilder<T> Append(T[] values) {
            if (values != null) {
                // This is a hand specialization of the 'AppendHelper' code below. 
                // We could have just called AppendHelper.  
                T[] chunkValues = m_ChunkValues;
                int chunkLength = m_ChunkLength;
                int valueLen = values.Length;
                int newCurrentIndex = chunkLength + valueLen;
                if (newCurrentIndex < chunkValues.Length)    // Use strictly < to avoid issue if count == 0, newIndex == length
                {
                    if (valueLen <= 2)
                    {
                        if (valueLen > 0)
                            chunkValues[chunkLength] = values[0];
                        if (valueLen > 1)
                            chunkValues[chunkLength + 1] = values[1];
                    }
                    else
                    {
                        Array.Copy(values,0,chunkValues,chunkLength, valueLen);
                    }
                    m_ChunkLength = newCurrentIndex;
                }
                else
                    Append(values, 0, values.Length);
            }
            return this;
        }

        public void CopyTo(int sourceIndex, T[] destination, int destinationIndex, int count) {
            if (destination == null) {
                throw new ArgumentNullException("destination");
            }

            if (count < 0) {
                throw new ArgumentOutOfRangeException("count", "Count must be positive");
            }

            if (destinationIndex < 0) {
                throw new ArgumentOutOfRangeException("destinationIndex", "Destination index must be positive");
            }

            if (destinationIndex > destination.Length - count) {
                throw new ArgumentException("Destination index is too high for all values");
            }

            if ((uint)sourceIndex > (uint)Length) {
                throw new ArgumentOutOfRangeException("sourceIndex", "Index does not exist in the instance");
            }

            if (sourceIndex > Length - count) {
                throw new ArgumentException("Source index is too high for this instance");
            }

            
            int sourceEndIndex = sourceIndex + count;
            var chunk = FindChunkForIndex(sourceEndIndex);
            int curDestIndex = destinationIndex + count;
            while (count > 0)
            {
                int chunkEndIndex = sourceEndIndex - chunk.m_ChunkOffset;
                if (chunkEndIndex >= 0)
                {
                    if (chunkEndIndex > chunk.m_ChunkLength)
                        chunkEndIndex = chunk.m_ChunkLength;

                    int chunkCount = count;
                    int chunkStartIndex = chunkEndIndex - count;
                    if (chunkStartIndex < 0)
                    {
                        chunkCount += chunkStartIndex;
                        chunkStartIndex = 0;
                    }
                    curDestIndex -= chunkCount;
                    count -= chunkCount;

                    // SafeCritical: we ensure that chunkStartIndex + chunkCount are within range of m_chunkChars
                    // as well as ensuring that curDestIndex + chunkCount are within range of destination
                    Array.Copy(chunk.m_ChunkValues, chunkStartIndex, destination, curDestIndex, chunkCount);
                }
                chunk = chunk.m_ChunkPrevious;
            }
        }

        // Inserts multiple copies of an array into this array builder at the specified position.
        // Existing items are shifted to make room for the new values.
        // The capacity is adjusted as needed. If value equals String.Empty, this
        // array builder is not changed. 
        public StructArrayBuilder<T> Insert(int index, T[] values, int count) {
            if (count < 0) {
                throw new ArgumentOutOfRangeException("count", "Count must be positive");
            }
            //Range check the index.
            int currentLength = Length;
            if ((uint)index > (uint)currentLength) {
                throw new ArgumentOutOfRangeException("index", "Index cannot be greater than Length");
            }

            //If values is null, empty or count is 0, do nothing. This is ECMA standard.
            if (values == null || values.Length == 0 || count == 0) {
                return this;
            }

            //Ensure we don't insert more items than we can hold, and we don't 
            //have any integer overflow in our inserted items.
            long insertingValues = (long) values.Length * count;
            if (insertingValues > MaxCapacity - this.Length) {
                throw new OutOfMemoryException();
            }

            StructArrayBuilder<T> chunk;
            int indexInChunk;
            MakeRoom(index, (int) insertingValues, out chunk, out indexInChunk, false);
            while (count > 0)
            {
                ReplaceInPlaceAtChunk(ref chunk, ref indexInChunk, values, values.Length);
                --count;
            }
            return this;
        }

        // Removes the specified items from this array builder.
        // The length of this array builder is reduced by 
        // length, but the capacity is unaffected.
        // 
        public StructArrayBuilder<T> Remove(int startIndex, int length) {
            if (length<0) {
                throw new ArgumentOutOfRangeException("length", "Length must be positive");
            }

            if (startIndex<0) {
                throw new ArgumentOutOfRangeException("startIndex", "Start index must be positive");
            }

            if (length > Length - startIndex) {
                throw new ArgumentOutOfRangeException("length", "Cannot remove after the end");
            }

            if (Length == length && startIndex == 0) {
                // Optimization.  If we are deleting everything  
                Length = 0;
                return this;
            }

            if (length > 0)
            {
                StructArrayBuilder<T> chunk;
                int indexInChunk;
                Remove(startIndex, length, out chunk, out indexInChunk);
            }
            return this;
        }


        /*====================================Insert====================================
        **
        ==============================================================================*/

        // Returns a reference to the StructArrayBuilder with ; value inserted into 
        // the buffer at index. Existing items are shifted to make room for the new text.
        // The capacity is adjusted as needed. If values is empty, the
        // StructArrayBuilder is not changed.
        // 
        public StructArrayBuilder<T> Insert(int index, T[] values) {
            if ((uint)index > (uint)Length) {
                throw new ArgumentOutOfRangeException("index", "Index does not exist");
            }

            if (values != null)
                Insert(index, values, 0, values.Length);
            return this;
        }

        // Returns a reference to the StructArrayBuilder with count items from 
        // value inserted into the buffer at index.  Existing items are shifted
        // to make room for the new text and capacity is adjusted as required.  If value is null, the StructArrayBuilder
        // is unchanged.  items are taken from value starting at position startIndex.
        
        public StructArrayBuilder<T> Insert(int index, T[] values, int startIndex, int count) {

            int currentLength = Length;
            if ((uint)index > (uint)currentLength) {
                throw new ArgumentOutOfRangeException("index", "Index does not exist");
            }

            //If they passed in a null array, just jump out quickly.
            if (values == null) {
                if (startIndex == 0 && count == 0)
                {
                    return this;
                }
                throw new ArgumentNullException("values");
            }

            //Range check the array.
            if (startIndex < 0) {
                throw new ArgumentOutOfRangeException("startIndex", "Start index must be positive");
            }

            if (count < 0) {
                throw new ArgumentOutOfRangeException("count", "Count must be positive");
            }

            if (startIndex > values.Length - count) {
                throw new ArgumentOutOfRangeException("count", "Count goes beyond the size of array");
            }

            if (count > 0)
            {
                Insert(index, values, 1);
            }
            return this;
        }

        public bool Equals(StructArrayBuilder<T> sb) 
        {
            if (sb == null)
                return false;
            if (Capacity != sb.Capacity || MaxCapacity != sb.MaxCapacity || Length != sb.Length)
                return false;
            if (sb == this)
                return true;

            var thisChunk = this;
            int thisChunkIndex = thisChunk.m_ChunkLength;
            var sbChunk = sb;
            int sbChunkIndex = sbChunk.m_ChunkLength;
            for (; ; )
            {
                // Decrement the pointer to the 'this' StructArrayBuilder
                --thisChunkIndex;
                --sbChunkIndex;

                while (thisChunkIndex < 0)
                {
                    thisChunk = thisChunk.m_ChunkPrevious;
                    if (thisChunk == null)
                        break;
                    thisChunkIndex = thisChunk.m_ChunkLength + thisChunkIndex;
                }

                // Decrement the pointer to the 'this' StructArrayBuilder
                while (sbChunkIndex < 0)
                {
                    sbChunk = sbChunk.m_ChunkPrevious;
                    if (sbChunk == null)
                        break;
                    sbChunkIndex = sbChunk.m_ChunkLength + sbChunkIndex;
                }

                if (thisChunkIndex < 0)
                    return sbChunkIndex < 0;
                if (sbChunkIndex < 0)
                    return false;
                if (!thisChunk.m_ChunkValues[thisChunkIndex].Equals(sbChunk.m_ChunkValues[sbChunkIndex]))
                    return false;
            }
        }

        public StructArrayBuilder<T> Replace(T[] oldValues, T[] newValues, int startIndex, int count)
        {
            int currentLength = Length;
            if ((uint)startIndex > (uint)currentLength) {
                throw new ArgumentOutOfRangeException("startIndex", "Start index cannot be greater than length");
            }

            if (count < 0 || startIndex > currentLength - count) {
                throw new ArgumentOutOfRangeException("count", "Count cannot be negative or imply replacing more values than there are");
            }
            if (oldValues == null)
            {
                throw new ArgumentNullException("oldValues");
            }
            if (oldValues.Length == 0)
            {
                throw new ArgumentException("Array is empty", "oldValues");
            }

            if (newValues == null)
                newValues = new T[0];

            int deltaLength = newValues.Length - oldValues.Length;

            int[] replacements = null;          // A list of replacement positions in a chunk to apply
            int replacementsCount = 0;

            // Find the chunk, indexInChunk for the starting point
            var chunk = FindChunkForIndex(startIndex);
            int indexInChunk = startIndex - chunk.m_ChunkOffset;
            while (count > 0)
            {
                // Look for a match in the chunk,indexInChunk pointer 
                if (StartsWith(chunk, indexInChunk, count, oldValues))
                {
                    // Push it on my replacements array (with growth), we will do all replacements in a
                    // given chunk in one operation below (see ReplaceAllInChunk) so we don't have to slide
                    // many times.  
                    if (replacements == null)
                        replacements = new int[5];
                    else if (replacementsCount >= replacements.Length)
                    {
                        int[] newArray = new int[replacements.Length * 3 / 2 + 4];     // grow by 1.5X but more in the begining
                        Array.Copy(replacements, newArray, replacements.Length);
                        replacements = newArray;
                    }
                    replacements[replacementsCount++] = indexInChunk;
                    indexInChunk += oldValues.Length;
                    count -= oldValues.Length;
                }
                else
                {
                    indexInChunk++;
                    --count;
                }

                if (indexInChunk >= chunk.m_ChunkLength || count == 0)       // Have we moved out of the current chunk
                {
                    // Replacing mutates the blocks, so we need to convert to logical index and back afterward. 
                    int index = indexInChunk + chunk.m_ChunkOffset;
                    int indexBeforeAdjustment = index;

                    // See if we accumulated any replacements, if so apply them 
                    ReplaceAllInChunk(replacements, replacementsCount, chunk, oldValues.Length, newValues);
                    // The replacement has affected the logical index.  Adjust it.  
                    index += ((newValues.Length - oldValues.Length) * replacementsCount);
                    replacementsCount = 0;

                    chunk = FindChunkForIndex(index);
                    indexInChunk = index - chunk.m_ChunkOffset;
                }
            }
            return this;
        }

        // Returns a StructArrayBuilder with all instances of oldValue replaced with 
        // newValue.  The size of the StructArrayBuilder is unchanged because we're only
        // replacing values.  If startIndex and count are specified, we 
        // only replace values in the range from startIndex to startIndex+count
        //
        public StructArrayBuilder<T> Replace(T oldValue, T newValue) {
            return Replace(oldValue, newValue, 0, Length);
        }
        public StructArrayBuilder<T> Replace(T oldValue, T newValue, int startIndex, int count) {

            int currentLength = Length;
            if ((uint)startIndex > (uint)currentLength) {
                throw new ArgumentOutOfRangeException("startIndex", "Start index cannot be greater than length");
            }

            if (count < 0 || startIndex > currentLength - count) {
                throw new ArgumentOutOfRangeException("count", "Count cannot be negative or imply replacing more values than there are");
            }

            int endIndex = startIndex + count;            
            var chunk = FindChunkForIndex(endIndex);
            for (; ; )
            {
                int endIndexInChunk = endIndex - chunk.m_ChunkOffset;
                int startIndexInChunk = startIndex - chunk.m_ChunkOffset;
                if (endIndexInChunk >= 0)
                {
                    int curInChunk = Math.Max(startIndexInChunk, 0);
                    int endInChunk = Math.Min(chunk.m_ChunkLength, endIndexInChunk);
                    while (curInChunk < endInChunk)
                    {
                        if (chunk.m_ChunkValues[curInChunk].Equals(oldValue))
                            chunk.m_ChunkValues[curInChunk] = newValue;
                        curInChunk++;
                    }
                }
                if (startIndexInChunk >= 0)
                    break;
                chunk = chunk.m_ChunkPrevious;
            }
            return this;
        }

        private void ReplaceAllInChunk(int[] replacements, int replacementsCount, StructArrayBuilder<T> sourceChunk, int removeCount, T[] values)
        {
            if (replacementsCount <= 0)
                return;

            // calculate the total amount of extra space or space needed for all the replacements.  
            int delta = (values.Length - removeCount) * replacementsCount;

            var targetChunk = sourceChunk;        // the target as we copy chars down
            int targetIndexInChunk = replacements[0];

            // Make the room needed for all the new items if needed. 
            if (delta > 0)
                MakeRoom(targetChunk.m_ChunkOffset + targetIndexInChunk, delta, out targetChunk, out targetIndexInChunk, true);
            // We made certain that items after the insertion point are not moved, 
            int i = 0;
            for (; ; )
            {
                // Copy in the new string for the ith replacement
                ReplaceInPlaceAtChunk(ref targetChunk, ref targetIndexInChunk, values, values.Length);
                int gapStart = replacements[i] + removeCount;
                i++;
                if (i >= replacementsCount)
                    break;

                int gapEnd = replacements[i];
                if (delta != 0)     // can skip the sliding of gaps if source an target string are the same size.  
                {
                    // Copy the gap data between the current replacement and the the next replacement
                    var chunkSubSet = new T[m_ChunkLength - gapStart];
                    Array.Copy(m_ChunkValues, gapStart, chunkSubSet, 0, chunkSubSet.Length);
                    ReplaceInPlaceAtChunk(ref targetChunk, ref targetIndexInChunk, chunkSubSet, gapEnd - gapStart);
                }
                else
                {
                    targetIndexInChunk += gapEnd - gapStart;
                }
            }

            // Remove extra space if necessary. 
            if (delta < 0)
                Remove(targetChunk.m_ChunkOffset + targetIndexInChunk, -delta, out targetChunk, out targetIndexInChunk);
        }

        /// <summary>
        /// Returns true if the array that is starts at 'chunk' and 'indexInChunk, and has a logical
        /// length of 'count' starts with the string 'values'. 
        /// </summary>
        private bool StartsWith(StructArrayBuilder<T> chunk, int indexInChunk, int count, T[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (count == 0)
                    return false;
                if (indexInChunk >= chunk.m_ChunkLength)
                {
                    chunk = Next(chunk);
                    if (chunk == null)
                        return false;
                    indexInChunk = 0;
                }

                // See if there no match, break out of the inner for loop
                if (chunk.m_ChunkValues[indexInChunk].Equals(values[i]))
                    return false;

                indexInChunk++;
                --count;
            }
            return true;
        }

        /// <summary>
        /// ReplaceInPlaceAtChunk is the logical equivalent of 'memcpy'.  Given a chunk and ann index in
        /// that chunk, it copies in 'count' items from 'value' and updates 'chunk, and indexInChunk to 
        /// point at the end of the items just copyied (thus you can splice in strings from multiple 
        /// places by calling this mulitple times.  
        /// </summary>
        private void ReplaceInPlaceAtChunk(ref StructArrayBuilder<T> chunk, ref int indexInChunk, T[] values, int count)
        {
            if (count != 0)
            {
                var valuesIdx = 0;
                for (; ; )
                {
                    int lengthInChunk = chunk.m_ChunkLength - indexInChunk;

                    int lengthToCopy = Math.Min(lengthInChunk, count);
                    Array.Copy(values, valuesIdx, chunk.m_ChunkValues, indexInChunk, lengthToCopy);

                    // Advance the index. 
                    indexInChunk += lengthToCopy;
                    if (indexInChunk >= chunk.m_ChunkLength)
                    {
                        chunk = Next(chunk);
                        indexInChunk = 0;
                    }
                    count -= lengthToCopy;
                    if (count == 0)
                        break;
                    valuesIdx += lengthToCopy;
                }
            }
        }


        /// <summary>
        /// Finds the chunk for the logical index (number of items in the whole StructArrayBuilder) 'index'
        /// YOu can then get the offset in this chunk by subtracting the m_BlockOffset field from 'index' 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        private StructArrayBuilder<T> FindChunkForIndex(int index)
        {
            // This is where we actually use the skip lists
            // 1. If it is a "odd" chunk and is not a match, then we need to go the previous one
            // 2. In the "even" chunk, based on how many references there are, the search can go deep down quickly
            // or slowly advance to the next odd chunk, until one of this previous odd chunk has 
            // higher order references, allowing to go faster.
            // Once we found the closest higher chunk, then we finish by just looking for the 1 previous chunk
            // Ex (k is the key in the dictionary, d is the distance in terms of references, d = 2 pow k. We count lookups and previous both as 1 operation ):
            // 1. Chunk to find=7, Current chunk=15
            //     The search will go 15 --(previous)-> 14 --(lookup k=1|d=2)-> 12 --(lookup k=2|d=4)-> 8 --(previous)->  7
            //     Result: 4 operations instead of 8 with just previous, one by one
            // 2. Chunk to find=1, Current chunk=16
            //     The search will go 16 --(lookup k=4|d=16, k=3|d=8)-> 8 --(lookup k=3|d=8, k=2|d=4)-> 4 --(lookup k=2|d=4, k=1|d=2)-> 2 --(previous)->  1
            //     Result: 7 operations instead of 15
            // 3. Chunk to find=8, Current chunk=17
            //     The search will go 17 --previous-> 16 --(lookup k=4|d=16, k=3|d=8)-> 8
            //     Result: 3 operations instead of 9
            //
            // However, testing shows that skiplist start to provide a performance boost
            // when the number of chunks gets bigger than 300-400, and is clearly faster by 500 chunks.
            // StructArrayBuilder this size are huge: that's around 1,000,000 elements
            // So here, we use this threshold to use simple, fast full list traversal on small/"normal"
            // size, and switch to skip list when over

            var ret = this;
            
            while (ret.m_ChunkOffset > index){

                // Check if we have to use the skip lists, and if we can actually do it
                if(ret.m_ChunkIndex > SkipListUsageMinimumIndex && 
                   ret.m_ChunkReferences != null && 
                   ret.m_ChunkReferences.Count > 0 )
                {
                    var curr = ret;

                    foreach(var k in ret.m_ChunkReferences.Keys){
                        if(ret.m_ChunkIndex == 0){
                            return ret;
                        }
                        if(ret.m_ChunkReferences == null){
                            ret = ret.m_ChunkPrevious;
                            break;
                        }else if(ret.m_ChunkReferences[k].m_ChunkOffset < index){
                            continue;
                        }else{
                            ret = ret.m_ChunkReferences[k];
                            break;
                        }
                    }
                    if(curr == ret){
                        ret = ret.m_ChunkPrevious;
                    }
                    // if all the referenced chunks are smaller (since the ret did not change), 
                    // then it can only mean that it is our previous one, since even the chunk at distance 2 would have been caught
                    if(ret == this){
                        ret = ret.m_ChunkPrevious;    
                    }
                }else{
                    ret = ret.m_ChunkPrevious;
                }
            }                
            return ret;
        }

        /// <summary>
        /// Finds the chunk that logically follows the 'chunk' chunk.  Chunks only persist the pointer to 
        /// the chunk that is logically before it, so this routine has to start at the this pointer (which 
        /// is a assumed to point at the chunk representing the whole StructArrayBuilder) and search
        /// until it finds the current chunk (thus is O(n)).  So it is more expensive than a field fetch!
        /// </summary>
        private StructArrayBuilder<T> Next(StructArrayBuilder<T> chunk)
        {
            if (chunk == this)
                return null;
            return FindChunkForIndex(chunk.m_ChunkOffset + chunk.m_ChunkLength);
        }

        /// <summary>
        /// Assumes that 'this' is the last chunk in the list and that it is full.  Upon return the 'this'
        /// block is updated so that it is a new block that has at least 'minBlockCharCount' items.
        /// that can be used to copy items into it.   
        /// </summary>
        private void ExpandByABlock(int minBlockCharCount)
        {
            if(Capacity != Length || minBlockCharCount <= 0){
                return;
            }

            if (minBlockCharCount + Length < minBlockCharCount || (minBlockCharCount + Length) > m_MaxCapacity)
                throw new ArgumentOutOfRangeException("requiredLength", "Cannot expand beyond max capacity");

            // Compute the length of the new block we need 
            // We make the new chunk at least big enough for the current need (minBlockCharCount)
            // But also as big as the current length (thus doubling capacity), up to a maximum
            // (so we stay in the small object heap, and never allocate really big chunks even if
            // the string gets really big. 
            int newBlockLength = Math.Max(minBlockCharCount, Math.Min(Length, MaxChunkSize));

            // Copy the current block to the new block, and initialize this to point at the new buffer. 
            var myPrevious = m_ChunkPrevious;
            m_ChunkPrevious = new StructArrayBuilder<T>(this);
            m_ChunkOffset += m_ChunkLength;
            m_ChunkLength = 0;
            
            //populate skip list for current chunk
            m_ChunkIndex++;
            if(useSkipLists){
                m_ChunkReferences = PopulateSkipLists(m_ChunkIndex, myPrevious);
            }

            // Check for integer overflow (logical buffer size > int.MaxInt)
            if (m_ChunkOffset + newBlockLength < newBlockLength)
            {
                m_ChunkValues = null;
                throw new OutOfMemoryException();
            }
            m_ChunkValues = new T[newBlockLength];

        }

        /// <summary>
        /// Used by ExpandByABlock to create a new chunk.  The new chunk is a copied from 'from'
        /// In particular the buffer is shared.  It is expected that 'from' chunk (which represents
        /// the whole list, is then updated to point to point to this new chunk. 
        /// </summary>
        private StructArrayBuilder(StructArrayBuilder<T> from)
        {
            m_ChunkLength = from.m_ChunkLength;
            m_ChunkOffset = from.m_ChunkOffset;
            m_ChunkValues = from.m_ChunkValues;
            m_ChunkPrevious = from.m_ChunkPrevious;
            m_MaxCapacity = from.m_MaxCapacity;

            m_ChunkIndex = from.m_ChunkIndex;
            m_ChunkReferences = from.m_ChunkReferences;

        }

        private SortedDictionary<ushort, StructArrayBuilder<T>> PopulateSkipLists(int chunkIndex, StructArrayBuilder<T> previous){

            SortedDictionary<ushort, StructArrayBuilder<T>> myReferences = null;

            if(chunkIndex >= 2 && chunkIndex % 2 == 0){

                myReferences = new SortedDictionary<ushort, StructArrayBuilder<T>>(new DescendingOrderComparer<ushort>());
                    
                ushort n = 1;
                var d = 0;
                StructArrayBuilder<T> currentChunk = previous;
                int currentIndex = chunkIndex;
                do{
                    d = (int)Math.Pow(2, n);
                    if(n == 1){
                        myReferences[n] = previous;
                    }else if(chunkIndex % d == 0 ){
                        var previousN =(ushort)(n-1);
                        if(previous.m_ChunkReferences.ContainsKey(previousN)){
                            myReferences[n] = previous.m_ChunkReferences[previousN];
                        } else{
                            do{
                                currentIndex-=2;
                                currentChunk = currentChunk.m_ChunkPrevious.m_ChunkPrevious;
                            }while(currentIndex > 0 && 
                                currentChunk != null && 
                                !currentChunk.m_ChunkReferences.ContainsKey(previousN));

                            myReferences[n] = currentChunk.m_ChunkReferences[previousN];
                        }
                    }
                    n++;
                }while(d < chunkIndex && previous.m_ChunkReferences != null );
            } 
            return myReferences;
        }

        /// <summary>
        /// Creates a gap of size 'count' at the logical offset (count of items in the whole string
        /// builder) 'index'.  It returns the 'chunk' and 'indexInChunk' which represents a pointer to
        /// this gap that was just created.  You can then use 'ReplaceInPlaceAtChunk' to fill in the
        /// chunk
        ///
        /// ReplaceAllChunks relies on the fact that indexes above 'index' are NOT moved outside 'chunk'
        /// by this process (because we make the space by creating the cap BEFORE the chunk).  If we
        /// change this ReplaceAllChunks needs to be updated. 
        ///
        /// If dontMoveFollowingChars is true, then the room must be made by inserting a chunk BEFORE the
        /// current chunk (this is what it does most of the time anyway)
        /// </summary>
        private void MakeRoom(int index, int count, out StructArrayBuilder<T> chunk, out int indexInChunk, bool doneMoveFollowingChars)
        {
            chunk = this;
            indexInChunk = -1;
            if(count <=0 || index < 0){
                return;
            }
            if (count + Length < count || count + Length > m_MaxCapacity)
                throw new ArgumentOutOfRangeException("requiredLength",  "Cannot expand beyond max capacity");

            while (chunk.m_ChunkOffset > index)
            {
                chunk.m_ChunkOffset += count;
                chunk = chunk.m_ChunkPrevious;
            }
            indexInChunk = index - chunk.m_ChunkOffset;

            // Cool, we have some space in this block, and you don't have to copy much to get it, go ahead
            // and use it.  This happens typically  when you repeatedly insert small strings at a spot
            // (typically the absolute front) of the buffer.    
            if (!doneMoveFollowingChars && chunk.m_ChunkLength <= DefaultCapacity * 2 && chunk.m_ChunkValues.Length - chunk.m_ChunkLength >= count)
            {
                for (int i = chunk.m_ChunkLength; i > indexInChunk; )
                {
                    --i;
                    chunk.m_ChunkValues[i + count] = chunk.m_ChunkValues[i];
                }
                chunk.m_ChunkLength += count;
                return;
            }

            // Allocate space for the new chunk (will go before this one)
            var newChunk = new StructArrayBuilder<T>(Math.Max(count, DefaultCapacity), chunk.m_MaxCapacity, chunk.m_ChunkPrevious);
            newChunk.m_ChunkLength = count;

            // Copy the head of the buffer to the  new buffer. 
            int copyCount1 = Math.Min(count, indexInChunk);
            if (copyCount1 > 0)
            {
                Array.Copy(chunk.m_ChunkValues, 0, newChunk.m_ChunkValues, 0, copyCount1);
                // Slide items in the current buffer over to make room. 
                int copyCount2 = indexInChunk - copyCount1;
                if (copyCount2 >= 0)
                {
                    Array.Copy(chunk.m_ChunkValues, copyCount1, chunk.m_ChunkValues, 0, copyCount2);
                    indexInChunk = copyCount2;
                }
            }

            chunk.m_ChunkPrevious = newChunk;           // Wire in the new chunk
            chunk.m_ChunkOffset += count;
            if (copyCount1 < count)
            {
                chunk = newChunk;
                indexInChunk = copyCount1;
            }

            //populate skip list for current chunk
            m_ChunkIndex++;
            if(useSkipLists){
                m_ChunkReferences = PopulateSkipLists(m_ChunkIndex, m_ChunkPrevious);
            }
        }

        /// <summary>
        ///  Used by MakeRoom to allocate another chunk.  
        /// </summary>
        private StructArrayBuilder(int size, int maxCapacity, StructArrayBuilder<T> previousBlock)
        {
            if(size <= 0 || maxCapacity <= 0){
                return;
            }
            m_ChunkValues = new T[size];
            m_MaxCapacity = maxCapacity;
            m_ChunkPrevious = previousBlock;
            if (previousBlock != null){
                m_ChunkOffset = previousBlock.m_ChunkOffset + previousBlock.m_ChunkLength;
            }
        }

        /// <summary>
        /// Removes 'count' items from the logical index 'startIndex' and returns the chunk and 
        /// index in the chunk of that logical index in the out parameters.  
        /// </summary>
        private void Remove(int startIndex, int count, out StructArrayBuilder<T> chunk, out int indexInChunk)
        {
            chunk = this;
            indexInChunk = -1;
            if(startIndex < 0 || startIndex >= Length){
                return;
            }

            int endIndex = startIndex + count;

            // Find the chunks for the start and end of the block to delete. 
            StructArrayBuilder<T> endChunk = null;
            int endIndexInChunk = 0;
            for (; ; )
            {
                if (endIndex - chunk.m_ChunkOffset >= 0)
                {
                    if (endChunk == null)
                    {
                        endChunk = chunk;
                        endIndexInChunk = endIndex - endChunk.m_ChunkOffset;
                    }
                    if (startIndex - chunk.m_ChunkOffset >= 0)
                    {
                        indexInChunk = startIndex - chunk.m_ChunkOffset;
                        break;
                    }
                }
                else
                {
                    chunk.m_ChunkOffset -= count;
                }
                chunk = chunk.m_ChunkPrevious;
            }

            int copyTargetIndexInChunk = indexInChunk;
            int copyCount = endChunk.m_ChunkLength - endIndexInChunk;
            if (endChunk != chunk)
            {
                copyTargetIndexInChunk = 0;
                // Remove the items after startIndex to end of the chunk
                chunk.m_ChunkLength = indexInChunk;

                // Remove the items in chunks between start and end chunk
                endChunk.m_ChunkPrevious = chunk;
                endChunk.m_ChunkOffset = chunk.m_ChunkOffset + chunk.m_ChunkLength;

                // If the start is 0 then we can throw away the whole start chunk
                if (indexInChunk == 0)
                {
                    endChunk.m_ChunkPrevious = chunk.m_ChunkPrevious;
                    chunk = endChunk;
                }
            }
            endChunk.m_ChunkLength -= (endIndexInChunk - copyTargetIndexInChunk);

            // SafeCritical: We ensure that endIndexInChunk + copyCount is within range of m_ChunkValues and
            // also ensure that copyTargetIndexInChunk + copyCount is within the chunk
            //
            // Remove any items in the end chunk, by sliding the items down. 
            if (copyTargetIndexInChunk != endIndexInChunk)  // Sometimes no move is necessary
                Array.Copy(endChunk.m_ChunkValues, endIndexInChunk, endChunk.m_ChunkValues, copyTargetIndexInChunk, copyCount);

        }

        public class DescendingOrderComparer<U> : IComparer<U> where U : IComparable,  IComparable<U> {
            public int Compare(U x, U y){
                return -1 * x.CompareTo(y);
            }
        }
    }
}