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
    public class StructArrayBuilder_Tests
    {
        [Fact]
        public void ShouldReturnArrayAsExpected ()
        {
            //Arrange
            var numberOfValuesPerPopulatingArrays = new int[]{12,89,123,1234578};
            var arrays = new List<decimal[]>();
            for(var i =0; i< numberOfValuesPerPopulatingArrays.Length; i++){
                var number = numberOfValuesPerPopulatingArrays[i];
                Random randNum = new Random(number + DateTime.Now.Millisecond);
                arrays.Add(Enumerable
                    .Repeat(0, number)
                    .Select(j => (decimal)randNum.Next(0, 10000000))
                    .ToArray());
            }
            var expectedResult = arrays.SelectMany( arr => arr).ToArray();
            
            //Action
            var builder = new StructArrayBuilder<decimal>();
            for(var i = 0; i< arrays.Count; i++){
                var arr = arrays[i];
                builder.Append(arr);
            }
            var result = builder.ToArray();

            //Assert
            Assert.NotNull(result);
            Assert.Equal(expectedResult.Length, result.Length);
            Assert.Equal(expectedResult, result);            
            
        }

        [Fact]
        public void CheckReadExecTime_Multiple ()
        {
            foreach(var numValues in new[]{
                //100,
                //1000,
                //10000, 
                //100000,                 
                //1000000, 
                //10000000, 
                100000000  
                }){
                CheckReadExecTime(false, numValues, action: "insert");
                CheckReadExecTime(true, numValues, action: "insert");
            }
        }

        // [Fact]
        // public void CheckReadExecTime_Single(){
        //     CheckReadExecTime();
        // }

        public void CheckReadExecTime ( 
            bool useSkipLists = true, int numValues = 10000, int chunkSize = 2000, 
            string action = "read", int percentOfChunksToAction = 33)
        {
            //Arrange
            if(numValues < 5 * chunkSize ){
                chunkSize = numValues / 5; 
            }
            var numberOfChunks = (int)(numValues / chunkSize);
            var numberOfValuesPerPopulatingArrays = Enumerable.Repeat(0, numberOfChunks).Select(n => chunkSize).ToArray();
            var arrays = new List<decimal[]>();
            for(var i =0; i< numberOfValuesPerPopulatingArrays.Length; i++){
                var number = numberOfValuesPerPopulatingArrays[i];
                Random randNum = new Random(number + DateTime.Now.Millisecond);
                arrays.Add(Enumerable
                    .Repeat(0, number)
                    .Select(j => (decimal)randNum.Next(0, 10000000))
                    .ToArray());
            }
            var expectedResult = arrays.SelectMany( arr => arr).ToArray();

            //Action
            var builder = new StructArrayBuilder<decimal>(useSkipLists);
            for(var i = 0; i< arrays.Count; i++){
                var arr = arrays[i];
                builder.Append(arr);
            }
            var chunksToAction = (int)(percentOfChunksToAction*numberOfChunks/100);
            switch(action){
                case "insert":                    
                    for(var l = 0; l<chunksToAction; l++){
                        builder.Insert(10, Enumerable.Repeat(0M, 10).Select( (d, idx) => (decimal)idx ).ToArray());
                    }
                    break;
                case "read":
                default:
                    break;
            }

            var stopWatch = new Stopwatch();
            var timeResults = new List<Tuple<int, double>>(numValues);
            var rnd = new Random(DateTime.Now.Millisecond);
            var increment = numValues > 10000 ? numValues / 10000 : 1;
            for(var j =0; j<numValues; j+=increment ){
                //Console.WriteLine("j: " + j);
                stopWatch.Start();
                var idx = j + (numValues > 10000 ? rnd.Next(0,increment-1) : 0 );
                var result = builder[ idx ];
                stopWatch.Stop();

                timeResults.Add(new Tuple<int, double>(idx, stopWatch.Elapsed.TotalMilliseconds));
                Console.WriteLine(idx+ ": " + stopWatch.Elapsed.TotalMilliseconds);
                stopWatch.Reset();
                switch(action){
                    case "read" :
                        Assert.Equal(expectedResult[idx], result);
                        break;
                    case "insert" :
                        if(idx < 10){
                            Assert.Equal(expectedResult[idx], result);
                        }else if( idx >= (10 + chunksToAction*10) ){
                            Assert.Equal(expectedResult [idx - (chunksToAction * 10) ], result);
                        } else {
                            Assert.Equal(idx % 10, result);
                        }
                        break;
                }
            }
 
            timeResults.RemoveAt(0);
            Console.WriteLine("Expected Array length:"+
             (action == "insert" ? (chunksToAction * 10) : 0 ) +  expectedResult.Length);
            Console.WriteLine("Builder length:"+builder.Length);
            Console.WriteLine("Builder Mem Size:"+builder.MemSize);
            Console.WriteLine("Max:"+timeResults.Select( t => t.Item2).Max());
            Console.WriteLine("Min:"+timeResults.Select( t => t.Item2).Min());
            Console.WriteLine("Avg:"+timeResults.Select( t => t.Item2).Average());

            var count = timeResults.Count;
            var tmp = Path.GetTempFileName();
            var filename = Path.GetFileNameWithoutExtension(tmp) + (useSkipLists? "_skip_" : "_normal_") + numValues.ToString() + ".csv";
            tmp = Path.Combine(Path.GetDirectoryName(tmp), filename);
            using(var fs = File.CreateText(tmp)){
                foreach(var tRes in timeResults){
                    fs.WriteLine(String.Join(",", tRes.Item1, tRes.Item2));
                }
                fs.Flush();
            }
        
            Console.WriteLine(tmp);
        }
    }
}
