
using System;
using System.IO;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using System.Security.Permissions;
 
namespace sharpeners {
    // A MemoryStream represents a Stream in memory (ie, it has no backing store).
    // This stream may reduce the need for temporary buffers and files in 
    // an application.  
    // 
    // There are two ways to create a MemoryStream.  You can initialize one
    // from an unsigned byte array, or you can create an empty one.  Empty 
    // memory streams are resizable, while ones created with a byte array provide
    // a stream "view" of the data.
    public class NonContiguousMemoryStream : Stream
    {
        private StructArrayBuilder<byte> _buffer;
        private int _position;     // read/write head.
        private bool _writable;    // Can user write to this stream?
        private bool _isOpen;      // Is this stream open or closed?
 

        [NonSerialized]
        private Task<int> _lastReadTask; // The last successful task returned from ReadAsync
 
        private const int MemStreamMaxLength = Int32.MaxValue;
 
        public NonContiguousMemoryStream() 
            : this(0) {
        }
        
        public NonContiguousMemoryStream(int capacity) {
            if (capacity < 0) {
                throw new ArgumentOutOfRangeException("capacity", "capacity cannot be negative");
            }
            Contract.EndContractBlock();
 
            _buffer = new StructArrayBuilder<byte>(capacity);
            _writable = true;
            _isOpen = true;
        }
        
        public NonContiguousMemoryStream(byte[] buffer) 
            : this(buffer, true) {
        }
        
        public NonContiguousMemoryStream(byte[] buffer, bool writable) {
            if (buffer == null){ throw new ArgumentNullException("buffer");}
            
            _buffer = new StructArrayBuilder<byte>(buffer);
            _writable = writable;
            _isOpen = true;
        }
        
        public NonContiguousMemoryStream(byte[] buffer, int index, int count) 
            : this(buffer, index, count, true, false) {
        }
        
        public NonContiguousMemoryStream(byte[] buffer, int index, int count, bool writable) 
            : this(buffer, index, count, writable, false) {
        }
    
        public NonContiguousMemoryStream(byte[] buffer, int index, int count, bool writable, bool publiclyVisible) {
            if (buffer==null)
                throw new ArgumentNullException("buffer");
            if (index < 0)
                throw new ArgumentOutOfRangeException("index", "index cannot be negative");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", "count cannot be negative");
            if (buffer.Length - index < count)
                throw new ArgumentException("count and index define positions greater than buffer length");
    
            _buffer = new StructArrayBuilder<byte>(buffer, index, count, index + count);
            _position = index + count;
            _writable = writable;
            _isOpen = true;
        }
    
        public override bool CanRead {
            get { return _isOpen; }
        }
        
        public override bool CanSeek {
            get { return _isOpen; }
        }
        
        public override bool CanWrite {
            get { return _writable; }
        }
 
        private void EnsureWriteable() {
            if (!CanWrite) {
                throw new NotSupportedException("Stream is not writable");
            }
        }
 
        protected override void Dispose(bool disposing)
        {
            try {
                if (disposing) {
                    _isOpen = false;
                    _writable = false;
                    // Don't set buffer to null - allow TryGetBuffer, GetBuffer & ToArray to work.

                    _lastReadTask = null;

                }
            }
            finally {
                // Call base.Close() to cleanup async IO resources
                base.Dispose(disposing);
            }
        }
        
        // returns a bool saying whether we allocated a new array.
        private bool EnsureCapacity(int value) {
            // Check for overflow
            if (value < 0)
                throw new IOException("Stream is too long");
            if (value > _buffer.Capacity) {
                _buffer.Capacity = value;
                return true;
            }
            return false;
        }
    
        public override void Flush() {
        }
        
        public override Task FlushAsync(CancellationToken cancellationToken) {
 
            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled(cancellationToken);
 
            try {
 
                Flush();
                return Task.CompletedTask;
        
            } catch(Exception ex) {
 
                return Task.FromException(ex);
            }
        }
         
        public virtual byte[] GetBuffer() {
            return _buffer.ToArray();
        }
 
        public virtual bool TryGetBuffer(out ArraySegment<byte> buffer) {
            buffer = new ArraySegment<byte>(_buffer.ToArray(), offset:0, count: _buffer.Length);
            return true;
        }
        internal int InternalEmulateRead(int count) {
            int n = (int)Length - _position;
            if (n > count) n = count;
            if (n < 0) n = 0;
 
            Contract.Assert(_position + n >= 0, "_position + n >= 0");  // len is less than 2^31 -1.
            _position += n;
            return n;
        }

        // Gets & sets the capacity (number of bytes allocated) for this stream.
        // The capacity cannot be set to a value less than the current length
        // of the stream.
        // 
        public virtual int Capacity {
            get { 
                if (!_isOpen){
                    throw new  ObjectDisposedException(null);
                } 
                return _buffer.Capacity;
            }
            set {
                if (!_isOpen){
                    throw new  ObjectDisposedException(null);
                } 
                _buffer.Capacity = value;
            }
        }        
 
        public override long Length {
            get {
                if (!_isOpen){
                    throw new  ObjectDisposedException(null);
                } 
                return _buffer.Length;
            }
        }
 
        public override long Position {
            get { 
                if (!_isOpen){
                    throw new  ObjectDisposedException(null);
                } 
                return _position;
            }
            set {
                if (value < 0)
                    throw new ArgumentOutOfRangeException("value", "value cannot be negative");
 
                if (!_isOpen){
                    throw new  ObjectDisposedException(null);
                } 

                if (value > MemStreamMaxLength)
                    throw new ArgumentOutOfRangeException("value", "value is too large");
                _position = (int)value;
            }
        }
 
        public override int Read([In, Out] byte[] buffer, int offset, int count) {
            if (buffer==null)
                throw new ArgumentNullException("buffer");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", "offset cannot be negative");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", "count cannot be negative");
            if (buffer.Length - offset < count)
                throw new ArgumentException("count and offset define positions greater than buffer length");
 
            if (!_isOpen){
                throw new  ObjectDisposedException(null);
            } 
 
            int n = (int)Length - _position;
            if (n > count) {
                n = count;
            }

            if (n <= 0){
                return 0;
            }
 
            Array.Copy(_buffer.ToArray(_position, n), 0, buffer, offset, n);

            _position += n;
 
            return n;
        }
 
        public override Task<int> ReadAsync(Byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer==null)
                throw new ArgumentNullException("buffer");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", "offset cannot be negative");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", "count cannot be negative");
            if (buffer.Length - offset < count)
                throw new ArgumentException("count and offset define positions greater than buffer length");
 
            if (!_isOpen){
                throw new  ObjectDisposedException(null);
            } 
            // If cancellation was requested, bail early
            if (cancellationToken.IsCancellationRequested) 
                return Task.FromCanceled<int>(cancellationToken);
 
            try
            {
                int n = Read(buffer, offset, count);
                var t = _lastReadTask;

                return (t != null && t.Result == n) ? t : (_lastReadTask = Task.FromResult<int>(n));
            }
            catch (OperationCanceledException oce)
            {
                return Task.FromCanceled<int>(oce.CancellationToken);
            }
            catch (Exception exception)
            {
                return Task.FromException<int>(exception);
            }
        }
 
        public override int ReadByte() {
            if (!_isOpen){
                throw new  ObjectDisposedException(null);
            } 
            
            if (_position >= (int)Length) return -1;
 
            return _buffer[_position++];
        }
 
 
        public override Task CopyToAsync(Stream destination, Int32 bufferSize, CancellationToken cancellationToken) {
 
            // This implementation offers beter performance compared to the base class version.
 
            // The parameter checks must be in sync with the base version:
            if (destination == null)
                throw new ArgumentNullException("destination");
            
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException("bufferSize", "bufferSize must be positive");
 
            if (!CanRead && !CanWrite)
                throw new ObjectDisposedException(null);
 
            if (!destination.CanRead && !destination.CanWrite)
                throw new ObjectDisposedException("destination");
 
            if (!CanRead)
                throw new NotSupportedException("Stream is not readable");
 
            if (!destination.CanWrite)
                throw new NotSupportedException("Stream is not writable");
 
 
            // If we have been inherited into a subclass, the following implementation could be incorrect
            // since it does not call through to Read() or Write() which a subclass might have overriden.  
            // To be safe we will only use this implementation in cases where we know it is safe to do so,
            // and delegate to our base class (which will call into Read/Write) when we are not sure.
            if (this.GetType() != typeof(NonContiguousMemoryStream))
                return base.CopyToAsync(destination, bufferSize, cancellationToken);
 
            // If cancelled - return fast:
            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled(cancellationToken);
           
            // Avoid copying data from this buffer into a temp buffer:
            //   (require that InternalEmulateRead does not throw,
            //    otherwise it needs to be wrapped into try-catch-Task.FromException like memStrDest.Write below)
 
            Int32 pos = _position;
            Int32 n = InternalEmulateRead((int)Length - _position);
 
            // If destination is not a memory stream, write there asynchronously:
            NonContiguousMemoryStream memStrDest = destination as NonContiguousMemoryStream;
            if (memStrDest == null)                 
                return destination.WriteAsync(_buffer.ToArray(pos, n), 0, n, cancellationToken);
           
            try {
 
                // If destination is a MemoryStream, CopyTo synchronously:
                memStrDest.Write(_buffer.ToArray(pos, n), 0, n);
                return Task.CompletedTask;
 
            } catch(Exception ex) {
                return Task.FromException(ex);
            }
        }
 
 
        public override long Seek(long offset, SeekOrigin loc) {
            if (!_isOpen){
                throw new  ObjectDisposedException(null);
            } 
 
            if (offset > MemStreamMaxLength)
                throw new ArgumentOutOfRangeException("offset", "offset is too large");
            switch(loc) {
            case SeekOrigin.Begin: {
                int tempPosition = unchecked((int)offset);
                if (tempPosition < 0)
                    throw new IOException("Cannot seek before origin");
                _position = tempPosition;
                break;
            }  
            case SeekOrigin.Current: {
                int tempPosition = unchecked( (int)offset);
                if (unchecked(_position + offset) < 0 || tempPosition < 0)
                    throw new IOException("Cannot seek before origin");
                _position = tempPosition;
                break;
            }    
            case SeekOrigin.End: {
                int tempPosition = unchecked(_buffer.Length + (int)offset);
                if ( unchecked(_buffer.Length + offset) < 0 || tempPosition < 0 )
                    throw new IOException("Cannot seek before origin");
                _position = tempPosition;
                break;
            }
            default:
                throw new ArgumentException("Invalid seek origin");
            }
 
            return _position;
        }
        
        // Sets the length of the stream to a given value.  The new
        // value must be nonnegative and less than the space remaining in
        // the array, Int32.MaxValue - origin
        // Origin is 0 in all cases other than a MemoryStream created on
        // top of an existing array and a specific starting offset was passed 
        // into the MemoryStream constructor.  The upper bounds prevents any 
        // situations where a stream may be created on top of an array then 
        // the stream is made longer than the maximum possible length of the 
        // array (Int32.MaxValue).
        // 
        public override void SetLength(long value) {
            if (value < 0 || value > Int32.MaxValue) {
                throw new ArgumentOutOfRangeException("value", "value is not a valid length");
            }
            EnsureWriteable();
 
            int newLength = (int)value;
            _buffer.Length = newLength;
 
        }
        
        public virtual byte[] ToArray() {
            return _buffer.ToArray();
        }
    
        public override void Write(byte[] buffer, int offset, int count) {
            if (buffer==null)
                throw new ArgumentNullException("buffer");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", "offset cannot be negative");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", "count cannot be negative");
            if (buffer.Length - offset < count)
                throw new ArgumentException("count and offset define positions greater than buffer length");
 
            EnsureWriteable();
 
            int i = _position + count;
            // Check for overflow
            if (i < 0){
                throw new IOException("Cannot write to stream. Buffer would overflow");
            }
            
            _buffer.Append(buffer, offset, count);

            _position = i;
 
        }

        public override Task WriteAsync(Byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            // If cancellation is already requested, bail early
            if (cancellationToken.IsCancellationRequested){ 
                return Task.FromCanceled(cancellationToken); 
            }
            try
            {
                Write(buffer, offset, count);
                return Task.CompletedTask;
            }
            catch (OperationCanceledException oce)
            {
                return Task.FromCanceled(oce.CancellationToken);
            }
            catch (Exception exception)
            {
                return Task.FromException(exception);
            }
        }
        
        public override void WriteByte(byte value) {
            if (!_isOpen){
                throw new  ObjectDisposedException(null);
            } 
            EnsureWriteable();
            
            _buffer[_position++] = value;
 
        }
    
        // Writes this MemoryStream to another stream.
        public virtual void WriteTo(Stream stream) {
            if (stream==null){
                throw new ArgumentNullException("stream");
            }
 
            if (!_isOpen){
                throw new  ObjectDisposedException(null);
            } 
            stream.Write(_buffer.ToArray(), 0, (int)Length);
        }
 
    }
}