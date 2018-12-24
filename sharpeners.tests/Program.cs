using System;
using System.Linq;

namespace sharpeners.tests{
    public class Program{

        public static void Main(string[] args){
            if(args.Length == 0){
                return;
            }
            var typeToTest = args[0].ToLowerInvariant();
            switch(typeToTest){
                case "structarraybuilder" : 
                    var lengthsToTest = args.Length >= 2 
                        ? args[1].Split(',', '|').Select( s => Convert.ToInt32(s)) 
                        : new []{ 
                            100,           //         100
                            1000,          //       1,000 
                            10000,         //      10,000
                            100000,        //     100,000
                            1000000,       //   1,000,000
                            10000000,      //  10,000,000
                            10000000,      // 100,000,000
                    };
                    var methodsToTest = args.Length >= 3 
                        ? args[2].Split(',', '|')
                        : new [] {"read", "insert", "remove", "replace"};
                    
                    var tests = new StructArrayBuilder_Tests();

                    foreach(var method in methodsToTest){
                        foreach(var lengthToTest in lengthsToTest){
                            tests.TestAndTrack(false, lengthToTest, action:method, logToConsole: true, trackPerf: true);
                            tests.TestAndTrack(true, lengthToTest, action:method, logToConsole: true, trackPerf: true);
                        }
                    }

                    break;
            }

            return;
        }
    }
}