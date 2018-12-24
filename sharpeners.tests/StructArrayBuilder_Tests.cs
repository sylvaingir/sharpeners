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
    public class TestState<T> where T : struct{
        public StructArrayBuilder<T> Builder {get; set;}
        public int NumberofChunksToAction {get;set;}
        public T[] ExpectedResult {get; set;}
    }

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
        public void Test_Read_SkipList(){
            TestAndTrack(true, action: "read", numValues: 50000);
            Console.WriteLine("Ran Test_Read_SkipList");
        }
        [Fact]
        public void Test_Read_Normal(){
            TestAndTrack(false, action: "read", numValues: 50000);
            Console.WriteLine("Ran Test_Read_Normal");
        }

        [Fact]
        public void Test_Insert_SkipList(){
            TestAndTrack(true, action: "insert", numValues: 50000);
            Console.WriteLine("Ran Test_Insert_SkipList");
        }
        [Fact]
        public void Test_Insert_Normal(){
            TestAndTrack(false, action: "insert", numValues: 50000);
            Console.WriteLine("Ran Test_Insert_Normal");
        }
        [Fact]
        public void Test_Remove_SkipList(){
            TestAndTrack(true, action: "remove", numValues: 50000);
            Console.WriteLine("Ran Test_Remove_SkipList");
        }
        [Fact]
        public void Test_Remove_Normal(){
            TestAndTrack(false, action: "remove", numValues: 50000);
            Console.WriteLine("Ran Test_Remove_Normal");
        }
        [Fact]
        public void Test_Replace_SkipList(){
            TestAndTrack(true, action: "replace", numValues: 50000);
            Console.WriteLine("Ran Test_Replace_SkipList");
        }
        [Fact]
        public void Test_Replace_Normal(){
            TestAndTrack(false, action: "replace", numValues: 50000);
            Console.WriteLine("Ran Test_Replace_Normal");
        }
        
        public TestState<decimal> Arrange_PrepArrayAndAppend(
            bool useSkipLists = true, int numValues = 10000, int chunkSize = 2000, int percentOfChunksToAction = 33) 
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

            return new TestState<decimal>(){
                Builder = builder,
                ExpectedResult = expectedResult,
                NumberofChunksToAction = (int)(percentOfChunksToAction*numberOfChunks/100)
            };
        }

        public void TestAndTrack ( 
            bool useSkipLists = true, int numValues = 10000, int chunkSize = 2000, 
            string action = "read", int percentOfChunksToAction = 33, 
            bool logToConsole = false, bool trackPerf = false)
        {
            //Arrange
            var prepdTest = Arrange_PrepArrayAndAppend(useSkipLists, numValues, chunkSize, percentOfChunksToAction);
            var builder = prepdTest.Builder;
            var chunksToAction = prepdTest.NumberofChunksToAction;
            var expectedResult = prepdTest.ExpectedResult;

            // Action
            switch(action){
                case "insert":                    
                    var valuesToInsert = Enumerable.Repeat(0M, 10).Select( (d, idx) => (decimal)idx ).ToArray();
                    for(var l = 0; l<chunksToAction; l++){
                        builder.Insert(10, valuesToInsert);
                    }
                    break;
                case "remove":                    
                    for(var l = 0; l<chunksToAction; l++){
                        builder.Remove(10, chunkSize);
                    }
                    break;
                case "replace":               
                    for(var l = 100000; l<=100100; l++){
                        builder.Replace(l, 0);
                    }
                    break;
                case "read":
                default:
                    break;
            }

            // Define perf tracking objects
            var stopWatch = new Stopwatch();
            var timeResults = new List<Tuple<int, double>>(numValues);

            // Track reading and Assert
            var rnd = new Random(DateTime.Now.Millisecond);
            var increment = numValues > 10000 ? numValues / 10000 : 1;
            for(var j =0; j<numValues; j+=increment ){
                
                if(trackPerf){ stopWatch.Start(); }

                var idx = j + (numValues > 10000 ? rnd.Next(0,increment-1) : 0 );
                if(idx >= builder.Length){
                    break;
                }
                var result = builder[ idx ];
                
                if(trackPerf){ 
                    stopWatch.Stop();
                    timeResults.Add(new Tuple<int, double>(idx, stopWatch.Elapsed.TotalMilliseconds));
                    if(logToConsole){ Console.WriteLine(idx+ ": " + stopWatch.Elapsed.TotalMilliseconds); }
                    stopWatch.Reset();
                }
                
                // Assert
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
                    case "remove" :
                        if(idx < 10){
                            Assert.Equal(expectedResult[idx], result);
                        }else if( idx >= 10 && idx < (numValues- chunksToAction*10)){
                            Assert.Equal(expectedResult [idx + (chunksToAction * chunkSize) ], result);
                        }
                        break;
                    case "replace" :
                        if(expectedResult[idx] >= 100000 && expectedResult[idx] <=100100){
                            Assert.Equal(0, result);
                        }else{
                            Assert.Equal(expectedResult [idx], result);
                        }
                        break;
                }
            }
            
            // Log test results
            // if(trackPerf){ timeResults.RemoveAt(0);}
            if(logToConsole){
                Console.WriteLine("Expected Array length:"+
                    (action == "insert" ? (chunksToAction * 10) : 0 ) +  expectedResult.Length);
                Console.WriteLine("Builder length:"+builder.Length);
                Console.WriteLine("Builder Mem Size:"+builder.MemSize);
                if(trackPerf){
                    Console.WriteLine("Max:"+timeResults.Select( t => t.Item2).Max());
                    Console.WriteLine("Min:"+timeResults.Select( t => t.Item2).Min());
                    Console.WriteLine("Avg:"+timeResults.Select( t => t.Item2).Average());
                }
            }

            // save exection time reading results
            if(trackPerf){
                var count = timeResults.Count;
                var tmp = Path.GetTempFileName();
                var filename = String.Join("_", Path.GetFileNameWithoutExtension(tmp), 
                    action,
                    ( useSkipLists ? "skip" : "normal"),
                    numValues.ToString()) + ".csv";
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
}
