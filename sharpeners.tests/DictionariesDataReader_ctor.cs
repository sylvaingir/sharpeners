using System;
using Xunit;
using sharpeners;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace sharpeners.tests
{
    public class DictionariesDataReader_ctor
    {
        [Fact]
        public void ShouldCreateConvertTheDataToIListOfIlistOfIDictionary()
        {
            //Arrange
            var rowsInT1 = new List<Dictionary<string, object>>(){
                new Dictionary<string, object>(){
                    { "str", "jljldklsd;"},
                    { "float", 8974651321.31684},
                    { "date", DateTime.Now},
                    { "char", 'f'},
                    { "boolean", false},
                    { "obj", new FormatException()}
                },
                new Dictionary<string, object>(){
                    { "str", "gpodljki;hdlkahsd"},
                    { "float", 0.3257865}
                },
                new Dictionary<string, object>(){
                    { "date", DateTime.Today},
                    { "char", 'u'}
                }
            };
            var rowsInT2 = new List<Dictionary<string, object>>(){
                new Dictionary<string, object>(){
                }
            };

            var tables = new List<IEnumerable<IDictionary<string, object>>>(){rowsInT1, rowsInT2};

            //Action
            var dReader = new DictionariesDataReader(tables);

            //Assert
            Assert.True(dReader.Read());
            Assert.Equal('f', dReader.GetValue(3)); //char

            Assert.True(dReader.Read());
            Assert.Null(dReader.GetValue(3));

            Assert.True(dReader.Read());
            Assert.Equal(DateTime.Today, dReader.GetValue(2)); // date

            Assert.False(dReader.Read());

            Assert.True(dReader.NextResult());
            Assert.False(dReader.Read());

            
            
        }
    }
}
