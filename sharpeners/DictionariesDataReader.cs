using System;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace sharpeners
{
    public class DictionariesDataReader : IDataReader
    {
        private IList<IList<IDictionary<string, object>>> _tables;
        private IList<DataTable> _schema;
        private IList<IDictionary<int, Tuple<string, Type>>> _fieldStructure;
        private IEnumerator<IList<IDictionary<string, object>>> _tablesEnumerator;
        private int? _tablesIdx;
        private IEnumerator<IDictionary<string, object>> _rowsEnumerator;
        private int? _rowsIdx;
        private bool _clearAfterRead;
        private bool? _isRowRead;

        public bool ClearAfterRead { 
            get {return _clearAfterRead;} 
            set{ 
                if(_tablesIdx.HasValue || _tablesEnumerator != null)
                { 
                    throw new InvalidOperationException("Cannot set ClearAfterRead while reading started");
                }
                _clearAfterRead = value;
            }
        }

        public DictionariesDataReader(IEnumerable<IEnumerable<IDictionary<string, object>>> tables, bool clearAfterRead = true)
        {
            _tables = tables == null ? new List<IList<IDictionary<string,object>>>() : tables.Select( t => (IList<IDictionary<string, object>>)t.ToList()).ToList();

            _clearAfterRead = clearAfterRead;

            _fieldStructure = new List<IDictionary<int, Tuple<string, Type>>>();
            foreach(var t in _tables)
            {
                _fieldStructure.Add(t.SelectMany( d => d.Select( kvp => new Tuple<string, Type>(kvp.Key, kvp.Value == null ? typeof(object) : kvp.Value.GetType() ) ) )
                    .Distinct().Select( (tup, idx) => new { idx = idx, tup = tup}).ToDictionary( o => o.idx, o => o.tup));
            }
        }

        public DictionariesDataReader(IEnumerable<IDictionary<string, object>> table, bool clearAfterRead=true)
            : this( new List<IEnumerable<IDictionary<string, object>>>(){ table }, clearAfterRead)
        {}
        
        public DictionariesDataReader(bool clearAfterRead=true)
            : this( (IEnumerable<IEnumerable<IDictionary<string, object>>>)null, clearAfterRead)
        {}

        #region IDataReader Impl
        public object this[int i] => GetValue(i);

        public object this[string name] => GetValue(GetOrdinal(name));

        public int Depth => 0;

        public bool IsClosed => _tables == null || !_tables.Any();

        public int RecordsAffected => -1;

        public int FieldCount { 
            get {
                StartReading(); 
                return _fieldStructure[_tablesIdx.Value].Count;
            } 
        }

        public void Close()
        {
            _tables = null;
            _tablesEnumerator = null;
            _tablesIdx = null;
            _rowsEnumerator = null;
            _rowsIdx = null;
            _fieldStructure = null;
            _schema = null;
        }

        public void Dispose()
        {
            Close();
        }

        public bool GetBoolean(int i)
        {
            return (bool)GetValue(i);
        }

        public byte GetByte(int i)
        {
            return (byte)GetValue(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public char GetChar(int i)
        {
            return (char)GetValue(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotSupportedException();
        }

        public string GetDataTypeName(int i)
        {
            return GetFieldType(i).Name;
        }

        public DateTime GetDateTime(int i)
        {
            return (DateTime)GetValue(i);
        }

        public decimal GetDecimal(int i)
        {
            return (decimal)GetValue(i);
        }

        public double GetDouble(int i)
        {
            return (double)GetValue(i);
        }

        public Type GetFieldType(int i)
        {
            StartReading();
            return _fieldStructure[_tablesIdx.Value][i].Item2;
        }

        public float GetFloat(int i)
        {
            return (float)GetValue(i);
        }

        public Guid GetGuid(int i)
        {
            return (Guid)GetValue(i);
        }

        public short GetInt16(int i)
        {
            return (short)GetValue(i);
        }

        public int GetInt32(int i)
        {
            return (int)GetValue(i);
        }

        public long GetInt64(int i)
        {
            return (long)GetValue(i);
        }

        public string GetName(int i)
        {
            StartReading();
            return _fieldStructure[_tablesIdx.Value][i].Item1;
        }

        public int GetOrdinal(string name)
        {
            if(String.IsNullOrWhiteSpace(name)){
                throw new ArgumentNullException("The column name is not specified.");
            }

            StartReading();
            var f = _fieldStructure[_tablesIdx.Value]
                .Where( kvp => name.Equals(kvp.Value.Item1, StringComparison.InvariantCultureIgnoreCase))
                .Select( kvp => kvp.Key).DefaultIfEmpty(-1).First();

            if(f < 0){
                throw new IndexOutOfRangeException("The name specified is not a valid column name.");
            }
            return f;
        }

        public DataTable GetSchemaTable()
        {
            StartReading();
            if(_schema == null)
            {
                _schema = new List<DataTable>();
            }

            if(_schema.Count < _tablesIdx + 1 || _schema[_tablesIdx.Value] == null)
            {
                return CreateSchemaTable(_tablesIdx.Value);
            }
            else
            {
                return _schema[_tablesIdx.Value];
            }
        }

        private DataTable CreateSchemaTable(int idx)
        {
            if(idx > _fieldStructure.Count -1){
                throw new IndexOutOfRangeException("The index provided does not correspond to any result");
            }
            var dt = new DataTable();
            dt.Columns.AddRange(
             new List<DataColumn>(){
                    new DataColumn("AllowDBNull", typeof(bool)),
                    new DataColumn("BaseSchemaName", typeof(string)),
                    new DataColumn("BaseCatalogName", typeof(string)),
                    new DataColumn("BaseTableName", typeof(string)),
                    new DataColumn("BaseColumnName", typeof(string)),
                    new DataColumn("ColumnName", typeof(string)),
                    new DataColumn("ColumnOrdinal", typeof(int)),
                    new DataColumn("ColumnSize", typeof(int)),
                    new DataColumn("DataType", typeof(Type)),
                    new DataColumn("IsAutoIncrement", typeof(bool)),
                    new DataColumn("IsKey", typeof(bool)),
                    new DataColumn("IsLong", typeof(bool)),
                    new DataColumn("IsReadOnly", typeof(bool)),
                    new DataColumn("IsRowVersion", typeof(bool)),
                    new DataColumn("IsUnique", typeof(bool)),
                    new DataColumn("NumericPrecision", typeof(int)),
                    new DataColumn("NumericScale", typeof(int)),
                    new DataColumn("ProviderType", typeof(Type))
                }.ToArray()
            );

            var fields = _fieldStructure[idx];
            foreach(var f in fields)
            {
                dt.Rows.Add(
                    true,
                    null, 
                    null,
                    null,
                    null,
                    f.Value.Item1,
                    f.Key, 
                    f.Value.Item2.IsValueType ? System.Runtime.InteropServices.Marshal.SizeOf(f.Value.Item2) : Int32.MaxValue,
                    f.Value.Item2,
                    false, 
                    false, 
                    false, 
                    false, 
                    false, 
                    false, 
                    255,
                    255,
                    f.Value.Item2
                );
            }

            dt.AcceptChanges();

            _schema[idx] = dt;
            return dt;
        }

        public string GetString(int i)
        {
            return (string)GetValue(i);
        }

        public object GetValue(int i)
        {
            StartReading();
            var name  = GetName(i);

            if(_rowsEnumerator == null){
                NextRow();
            }
            return _rowsEnumerator.Current.ContainsKey(name) ? _rowsEnumerator.Current[name] : null;
        }

        public int GetValues(object[] values)
        {
            StartReading();
            var myValues = _rowsEnumerator.Current.OrderBy( kvp => kvp.Key).Select( kvp => kvp.Value).ToArray();
            var count = Math.Min(myValues.Length, values.Length);
            Array.Copy(myValues, values, count);
            return count;
        }

        public bool IsDBNull(int i)
        {
            return GetValue(i) == null;
        }

        private void StartReading()
        {
            if(!_tablesIdx.HasValue || _tablesEnumerator == null)
            {
                if(!NextResult())
                {
                    throw new InvalidOperationException("Reader failed to start");
                }
            }
        }
        public bool NextResult()
        {
            if(IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed.");
            }

            if(_tablesEnumerator == null || !_tablesIdx.HasValue)
            {
                _tablesEnumerator = _tables.GetEnumerator();
                _tablesIdx = -1;
                if(_tablesEnumerator == null){
                    return false;
                }
            }

            var didMove =_tablesEnumerator.MoveNext();
            ++_tablesIdx; 

            if(didMove && _tablesEnumerator.Current != null)
            {
                _rowsEnumerator = null;
                _rowsIdx = -1;
                _isRowRead = null;
            }

            return didMove;

        }

        private bool NextRow(){
            
            if(_rowsEnumerator == null || !_rowsIdx.HasValue)
            {
                _rowsEnumerator = _tablesEnumerator.Current.GetEnumerator();
                if(_rowsEnumerator == null || _fieldStructure[_tablesIdx.Value].Count == 0)
                {
                    return false;
                }
            }

            var didMove = false;
            if(!_isRowRead.HasValue || _isRowRead.Value)
            {
                if(_clearAfterRead && _rowsEnumerator.Current != null)
                {
                    _rowsEnumerator.Current.Clear();
                }

                didMove = _rowsEnumerator.MoveNext();
            }

            return didMove;
        }

        public bool Read()
        {
            StartReading();
            var isRead = (_isRowRead.HasValue && !_isRowRead.Value ) || NextRow();
            _isRowRead = true;

            return isRead;
        }
        #endregion
    }
        
}
