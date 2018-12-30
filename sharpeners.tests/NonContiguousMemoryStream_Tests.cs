using System;
using Xunit;
using sharpeners;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.IO;

namespace sharpeners.tests
{

    public class NonContiguousMemoryStream_Tests
    {
        [Fact]
        public void WriteAndReadAndToArray ()
        {
            var randNum = new Random(DateTime.Now.Millisecond);
            var expectedResult = Enumerable
                    .Repeat(0, 10000)
                    .Select(j => (byte)randNum.Next(0, 255))
                    .ToArray();

            var ncms = new NonContiguousMemoryStream();

            var written = 0;
            while(written < expectedResult.Length){
                var toWrite = Math.Min( expectedResult.Length - written, 2048);
                ncms.Write(expectedResult, written, toWrite);
                written += toWrite;
            }

            Assert.Equal(expectedResult.Length, written);
            Assert.Equal(expectedResult.Length, ncms.Length);

            var result = new byte[ncms.Length];
            ncms.Position = 0;

            var read = 0;
            while(read < expectedResult.Length){
                var toRead = Math.Min( expectedResult.Length - read, 1024);
                var readBytes = ncms.Read(result, read, toRead);
                read += readBytes;
            }

            Assert.Equal(expectedResult.Length, read);
            Assert.Equal(expectedResult, ncms.ToArray());

            for(var i = 0; i < expectedResult.Length; i++){
                Assert.Equal(expectedResult[i], result[i]);
            }

            Console.WriteLine("Ran NonContiguousMemoryStream_Tests.WriteAndRead");
        }
    }
}