using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;
using System.Threading;

namespace ProjectGraphs
{
    public class Program
    {
        const int VALUE = 99;

        static void Main(string[] args)
        {
            for (int i = 0; i < 10; i++)
            {
                if (i == 5)
                {
                    throw new Exception("");
                }
                Console.WriteLine(i);
            }

            //var graph = Generate2CrossGraph(1000,LongOperation, LongOperationArray);
            //Execute2CrossGraph(graph, false);

            //var graph = Generate3CrossGraph(100, LongOperationArray);
            //Execute3CrossGraph(graph, false);

            //var graph = GenerateSumSoFarGraph(100, LongOperationArray);
            //ExecuteSumSoFarGraph(graph, false);

            //var graph = Generate3AverageGraph(1000, LongOperationArray);
            //Execute3AverageGraph(graph, false);

            //var graph = GenerateVerticalGraph(1000, LongOperation);
            //ExecuteVerticalGraph(graph);

            //var graph = GenerateSumGraph(10, LongOperationArray);
            //ExecuteSumGraph(graph, false);
        }


        public static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> GenerateSumSoFarGraph(int depth,
            Func<long[], long> f)
        {
            Stopwatch sw = new Stopwatch();
            sw.Restart();

            // create
            var firstLayer = new BroadcastBlock<long>[depth];
            var secondLayer = new IPropagatorBlock<long, long>[depth];
            for (int i = 0; i < depth; i++)
            {
                firstLayer[i] = new BroadcastBlock<long>(null);
                secondLayer[i] = CreateSumSoFarNode(i + 1, f);
            }

            // linking
            for (int i = 0; i < depth; i++)
            {
                for (int k = i; k < depth; k++)
                {
                    firstLayer[i].LinkTo(secondLayer[k]);
                }
            }

            sw.Stop();
            Console.WriteLine("Creation: {0}", sw.Elapsed);

            return new Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]>(firstLayer, secondLayer);
        }

        public static long ExecuteSumSoFarGraph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> graph,
            bool isTestForCorrectness)
        {
            //int cntTrials = 70;
            //for (int i = 0; i < cntTrials; i++)
            //{
            //    Stopwatch sw = new Stopwatch();
            //    sw.Restart();

            //    // version 3
            //    int cntLayerNodes = graph.Item1.Length;
            //    var tasks1 = new List<Task>();
            //    for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
            //    {
            //        int k = nodeId;
            //        tasks1.Add(graph.Item1[k].SendAsync(1));
            //    }

            //    var tasks2 = new List<Task>();
            //    for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
            //    {
            //        int k = nodeId;
            //        tasks2.Add(graph.Item2[k].ReceiveAsync());
            //    }

            //    Task.WaitAll(tasks1.ToArray());
            //    Task.WaitAll(tasks2.ToArray());

            //    // version 2
            //    //int cntLayerNodes = graph.Item1.Length;
            //    //Parallel.For(0, cntLayerNodes, nodeId => { graph.Item1[nodeId].Post(1); });
            //    //Parallel.For(0, cntLayerNodes, nodeId => { graph.Item2[nodeId].Receive(); });

            //    // version 1
            //    //int cntLayerNodes = graph.Item1.Length;
            //    //for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
            //    //{
            //    //    graph.Item1[nodeId].Post(1);
            //    //}

            //    //for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
            //    //{
            //    //    graph.Item2[nodeId].Receive();
            //    //}

            //    sw.Stop();
            //    Console.WriteLine("Execution: {0} - {1}", i + 1, sw.Elapsed);
            //}

            long result = 0;
            if (isTestForCorrectness)
            {
                int cntLayerNodes = graph.Item1.Length;
                for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
                {
                    graph.Item1[nodeId].Post(1);
                }

                for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
                {
                    result += graph.Item2[nodeId].Receive();
                }
            }
            else
                BenchMark("Sum so far", (x) =>
                {
                    int cntLayerNodes = graph.Item1.Length;
                    var tasks1 = new List<Task>();
                    for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
                    {
                        int k = nodeId;
                        tasks1.Add(graph.Item1[k].SendAsync(x));
                    }

                    var tasks2 = new List<Task<long>>();
                    for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
                    {
                        int k = nodeId;
                        tasks2.Add(graph.Item2[k].ReceiveAsync());
                    }

                    Task.WhenAll(tasks1.ToArray());
                    Task.WaitAll(tasks2.ToArray());
                    // result?

                    return 1;
                });

            return result;
        }

        public static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> Generate3AverageGraph(int depth,
            Func<long[], long> f)
        {
            Stopwatch sw = new Stopwatch();
            sw.Restart();

            // create nodes
            IPropagatorBlock<long, long>[] firstLayer = new IPropagatorBlock<long, long>[depth];
            for (int i = 0; i < depth; i++)
            {
                firstLayer[i] = new BroadcastBlock<long>(x => x);
            }

            IPropagatorBlock<long, long>[] secondLayer = new IPropagatorBlock<long, long>[depth - 2];
            for (int i = 0; i < depth - 2; i++)
            {
                secondLayer[i] = Create3AverageNode(f);
            }

            // link nodes - iterate over the 2nd layer
            for (int i = 0; i < depth - 2; i++)
            {
                firstLayer[i].LinkTo(secondLayer[i]);
                firstLayer[i + 1].LinkTo(secondLayer[i]);
                firstLayer[i + 2].LinkTo(secondLayer[i]);
            }

            sw.Stop();
            Console.WriteLine("Creation: {0}", sw.Elapsed);

            return new Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]>(firstLayer, secondLayer);
        }

        public static long Execute3AverageGraph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> graph,
            bool isTestForCorrectness)
        {
            //int depth = graph.Item1.Length;
            //Random r = new Random();
            //int cntAttempts = 60;
            //for (int i = 0; i < cntAttempts; i++)
            //{
            //    Stopwatch sw = new Stopwatch();
            //    sw.Restart();

            //    for (int col = 0; col < depth; col++)
            //    {
            //        graph.Item1[col].Post(1);
            //    }

            //    // version 2
            //    //var tasks = new List<Task<long>>();
            //    //for (int col = 0; col < depth - 2; col++)
            //    //{
            //    //    tasks.Add(graph.Item2[col].ReceiveAsync());
            //    //}

            //    //Task.WaitAll(tasks.ToArray());

            //    // version 1
            //    for (int col = 0; col < depth - 2; col++)
            //    {
            //        graph.Item2[col].Receive();
            //    }

            //    sw.Stop();
            //    Console.WriteLine("Execution time {0}: {1}", i + 1, sw.Elapsed);
            //}

            long result = 0;
            if (isTestForCorrectness)
            {
                int depth = graph.Item1.Length;
                for (int col = 0; col < depth; col++)
                {
                    graph.Item1[col].Post(1);
                }

                for (int col = 0; col < depth - 2; col++)
                {
                    result += graph.Item2[col].Receive();
                }
            }
            else
                BenchMark("3 average graph", x =>
                {
                    int depth = graph.Item1.Length;

                    var SendingTasks = new List<Task>();
                    for (int col = 0; col < depth; col++)
                    {
                        //graph.Item1[col].Post(x);
                        SendingTasks.Add(graph.Item1[col].SendAsync(x));
                    }

                    var ReceivingTasks = new List<Task<long>>();
                    for (int col = 0; col < depth - 2; col++)
                    {
                        ReceivingTasks.Add(graph.Item2[col].ReceiveAsync());
                    }

                    Task.WaitAll(SendingTasks.ToArray());
                    Task.WaitAll(ReceivingTasks.ToArray());

                    return x;
                });

            return result;
        }

        public static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> Generate2CrossGraph(int depth,
            Func<long> f1, Func<long[], long> f2)
        {
            Stopwatch sw = new Stopwatch();
            sw.Restart();

            const int WIDTH = 2;
            IPropagatorBlock<long, long>[,] graph = new IPropagatorBlock<long, long>[depth, WIDTH];

            // creation
            for (int row = 0; row < depth; row++)
            {
                if (row == 0) // first row
                {
                    graph[row, 0] = new BroadcastBlock<long>(x => x);
                    graph[row, 1] = new BroadcastBlock<long>(x => x);
                }
                else // other rows 
                {
                    graph[row, 0] = Create2CrossNode_1(f1);
                    graph[row, 1] = Create2CrossNode_2(f2);  //new TransformBlock<long, long>(x => LongOperation()); 
                }
            }

            // linking
            for (int row = 0; row < depth - 1; row++)
            {
                graph[row, 0].LinkTo(graph[row + 1, 0]);
                graph[row, 0].LinkTo(graph[row + 1, 1]);

                graph[row, 1].LinkTo(graph[row + 1, 1]);
            }

            // return the first and the last layer
            IPropagatorBlock<long, long>[] firstLayer = new BroadcastBlock<long>[WIDTH];
            firstLayer[0] = graph[0, 0]; // only copies the ref.!!!
            firstLayer[1] = graph[0, 1]; // only copies the ref.!!!

            IPropagatorBlock<long, long>[] lastLayer = new IPropagatorBlock<long, long>[WIDTH];
            lastLayer[0] = graph[depth - 1, 0]; // only copies the ref.!!!
            lastLayer[1] = graph[depth - 1, 1]; // only copies the ref.!!!

            sw.Stop();
            Console.WriteLine("Creation {0}", sw.Elapsed);

            return new Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]>(firstLayer, lastLayer);
        }

        public static long Execute2CrossGraph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> graph,
            bool isTestForCorrectness)
        {
            // ??? Posting by using Parallel.For()
            // ? Receive() or AsynchReceive()
            // exeuction is fast

            //int cntAttempts = 50;
            //for (int i = 0; i < cntAttempts; i++)
            //{
            //    Stopwatch sw = new Stopwatch();
            //    sw.Restart();

            //    graph.Item1[0].Post(1);
            //    graph.Item1[1].Post(1);

            //    graph.Item2[0].Receive(); // should finish first
            //    graph.Item2[1].Receive(); // should take bit longer time

            //    sw.Stop();
            //    Console.WriteLine("Execution time {0}: {1}", i + 1, sw.Elapsed);
            //}

            long result = 0;
            if (isTestForCorrectness)
            {
                graph.Item1[0].Post(1);
                graph.Item1[1].Post(1);

                result += graph.Item2[0].Receive(); // should finish first
                result += graph.Item2[1].Receive(); // should take longer time
            }
            else
                BenchMark("2 cross graph", x =>
                {
                    graph.Item1[0].Post(1);
                    graph.Item1[1].Post(1);

                    long res = 0;
                    res += graph.Item2[0].Receive(); // should finish first
                    res += graph.Item2[1].Receive(); // should take longer time
                    //Console.WriteLine(res);
                    return res;
                });

            return result;
        }

        public static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> Generate3CrossGraph(int cntNodes,
            Func<long[], long> f)
        {
            int cntRows = cntNodes;
            int cntCols = cntNodes;
            IPropagatorBlock<long, long>[,] matrix = new IPropagatorBlock<long, long>[cntRows, cntCols];

            Stopwatch sw = new Stopwatch();
            sw.Start();

            // creation
            for (int row = 0; row < cntRows; row++)
            {
                if (row == 0)
                {
                    for (int col = 0; col < cntCols; col++)
                    {
                        matrix[row, col] = new BroadcastBlock<long>(x => x);
                    }
                }

                if (row > 0)
                {
                    for (int col = 0; col < cntCols; col++)
                    {
                        if (col == 0 || col == cntCols - 1)
                        {
                            matrix[row, col] = Create3CrossNode(2, f);
                        }
                        else
                        {
                            matrix[row, col] = Create3CrossNode(3, f);
                        }
                    }
                }
            }

            // linking
            for (int row = 0; row < cntRows - 1; row++)
            {
                for (int col = 0; col < cntCols; col++)
                {
                    if (col == 0)
                    {
                        matrix[row, col].LinkTo(matrix[row + 1, col]);
                        matrix[row, col].LinkTo(matrix[row + 1, col + 1]);
                    }

                    if (col > 0 && col < cntCols - 1)
                    {
                        matrix[row, col].LinkTo(matrix[row + 1, col - 1]);
                        matrix[row, col].LinkTo(matrix[row + 1, col]);
                        matrix[row, col].LinkTo(matrix[row + 1, col + 1]);
                    }

                    if (col == cntCols - 1)
                    {
                        matrix[row, col].LinkTo(matrix[row + 1, col - 1]);
                        matrix[row, col].LinkTo(matrix[row + 1, col]);
                    }
                }
            }

            // first layer
            var firstLayer = new IPropagatorBlock<long, long>[cntCols];
            for (int i = 0; i < cntCols; i++)
            {
                firstLayer[i] = matrix[0, i];
            }

            //last layer
            var lastLayer = new IPropagatorBlock<long, long>[cntCols];
            for (int i = 0; i < cntCols; i++)
            {
                lastLayer[i] = matrix[cntRows - 1, i];
            }

            sw.Stop();
            Console.WriteLine("Creation time: {0}", sw.Elapsed);

            return new Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]>(firstLayer, lastLayer);
        }

        public static long Execute3CrossGraph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> graph,
            bool isTestForCorrectness)
        {
            int cntNodes = graph.Item1.Count();
            int cntAttempts = 50;
            long finalSum = 0;
            double elapsedTimeSum = 0;

            //List<long> intsList = new List<long>();
            //for (int i = 0; i < cntAttempts; i++)
            //{
            //    Stopwatch sw = Stopwatch.StartNew();
            //    intsList.Clear();

            //    //var tasks = new List<Task<bool>>();
            //    //foreach (var node in graph.Item1)
            //    //{
            //    //    tasks.Add(node.SendAsync(1));
            //    //}
            //    //Task.WaitAll(tasks.ToArray());

            //    foreach (IPropagatorBlock<long, long> node in graph.Item1)
            //    {
            //        node.Post((long)1);
            //    }

            //    finalSum = 0;
            //    foreach (IPropagatorBlock<long, long> node in graph.Item2)
            //    {
            //        //long item = node.Receive(); // node.Receive(new TimeSpan(5000000));
            //        //intsList.Add(item);
            //        //finalSum += item;

            //        finalSum += node.Receive();
            //    }

            //    //Thread.Sleep(10);

            //    //Task.WaitAll(tasks.ToArray());

            //    //sw.Stop();
            //    //elapsedTimeSum = elapsedTimeSum + sw.ElapsedMilliseconds;
            //    Console.WriteLine(sw.ElapsedMilliseconds);
            //    //Console.WriteLine("Execution time {0}: {1}, Sum: {2}", i + 1, sw.Elapsed, finalSum);
            //    //String s = "" ;
            //    //intsList.ForEach(x => s = s + x + ", ");
            //    //Console.WriteLine(elapsedTimeSum);
            //}
            //Console.WriteLine(elapsedTimeSum);

            long result = 0;
            if (isTestForCorrectness)
            {
                for (int i = 0; i < graph.Item1.Length; i++)
                {
                    graph.Item1[i].Post(1);
                }

                foreach (var node in graph.Item2)
                {
                    result += node.Receive();
                }
            }
            else
                // =============== Benchmark ==========
                BenchMark("3 cross graph", x =>
                {
                    //Thread.Sleep(100);
                    //var tasksSend = new List<Task>();
                    //foreach (var node in graph.Item1)
                    for (int i = 0; i < graph.Item1.Length; i++)
                    {
                        //tasksSend.Add(node.SendAsync(1));
                        //Console.WriteLine("sdf");
                        graph.Item1[i].Post(1);
                    }

                    //Thread.Sleep(10);
                    //Task.WaitAll(tasksSend.ToArray());
                    //var tasksReceive = new List<Task<long>>();
                    long res = 0;
                    foreach (var node in graph.Item2)
                    {
                        //tasksReceive.Add(node.ReceiveAsync());                    
                        res += node.Receive();
                    }

                    //long res = 1;

                    //Task.WhenAll(tasksReceive.ToArray()).ContinueWith(t =>
                    //{
                    //    long sum = t.Result.ToList().Sum();                    
                    //    Interlocked.Add(ref res, sum);
                    //}).Wait();
                    // res += t.Result.ToList().Sum()

                    //Task.WaitAll(tasksReceive.ToArray());
                    //foreach (var task in tasksReceive)
                    //{
                    //    long taskres = (int)task.Result;
                    //    //Console.WriteLine("tskres = " + taskres);
                    //    res += (int)taskres;
                    //}

                    //Thread.Sleep(10);
                    //Console.WriteLine(res);

                    return (int)res / 5;
                });
            // =============== Benchmark ==========

            //return finalSum;
            return result;
        }

        public static long ExecuteSumGraph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long[], long>> graph,
            bool isTestForCorrectness)
        {
            // Posting by using Parallel.For()
            //Random r = new Random();
            //int cntNodes = graph.Item1.Length;
            //int cntAttempts = 5;
            //for (int i = 0; i < cntAttempts; i++)
            //{
            //    Stopwatch sw = new Stopwatch();
            //    sw.Restart();
            //    //foreach (var idx in Enumerable.Range(0, cntNodes))
            //    Parallel.For(0, cntNodes, idx =>
            //    {
            //        graph.Item1[idx].Post(r.Next(1000));
            //    });

            //    var res = graph.Item2.Receive();
            //    sw.Stop();
            //    Console.WriteLine("Execution time {0}: {1}", i + 1, sw.Elapsed);
            //}

            long result = 0;
            if (isTestForCorrectness)
            {
                foreach (var node in graph.Item1)
                {
                    //sendingTasks.Add(node.SendAsync(x));
                    node.Post(1);
                }

                result = graph.Item2.Receive();
            }
            else
                BenchMark("Sum graph", (x) =>
                {
                    //List<Task> sendingTasks = new List<Task>();
                    foreach (var node in graph.Item1)
                    {
                        //sendingTasks.Add(node.SendAsync(x));
                        node.Post(x);
                    }

                    //Task.WaitAll(sendingTasks.ToArray());
                    var res = graph.Item2.Receive();

                    return x;
                });

            return result;
        }

        public static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long[], long>> GenerateSumGraph(int cntNodes,
            Func<long[], long> f)
        {
            Stopwatch sw = new Stopwatch();
            sw.Restart();

            // creation
            IPropagatorBlock<long, long>[] nodes = new BufferBlock<long>[cntNodes];
            for (int i = 0; i < cntNodes; i++)
            {
                nodes[i] = new BufferBlock<long>();
            }

            BatchBlock<long> batcher = new BatchBlock<long>(cntNodes, new GroupingDataflowBlockOptions() { Greedy = true });
            IPropagatorBlock<long[], long> transformer = new TransformBlock<long[], long>(x => f(x));  // f = LongOperationArray

            // linking
            foreach (var item in nodes)
            {
                item.LinkTo(batcher);
            }
            batcher.LinkTo(transformer);
            Console.WriteLine("Creation time: {0}", sw.Elapsed);
            return new Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long[], long>>(nodes, transformer);
        }

        public static void ExecuteVerticalGraph(Tuple<IPropagatorBlock<long, long>, IPropagatorBlock<long, long>> vertGraph)
        {
            //const int n = 10;
            //Stopwatch sw = new Stopwatch();
            //for (int i = 0; i < n-1; i++)
            //{
            //    sw.Restart();
            //    vertGraph.Item1.Post(232);
            //    int res = vertGraph.Item2.Receive();
            //    sw.Stop();
            //    Console.WriteLine("Execution time {0}: {1}. Result = {2}", i + 1, sw.Elapsed, res);
            //}

            BenchMark("Vertical vector graph", (x) =>
            {
                vertGraph.Item1.Post(x);
                long res = vertGraph.Item2.Receive();
                return res;
            });
        }

        public static Tuple<IPropagatorBlock<long, long>, IPropagatorBlock<long, long>> GenerateVerticalGraph(int cntNodes,
            Func<long> f)
        {
            /*
             *  cntNodes = 1_000_000 -> out of memory exception
             *  MaxDegreeOfParallelism = 4 - doesn't help. We put 1 item and check. No many items in the buffer
             */

            Stopwatch sw = new Stopwatch();
            sw.Start();

            // creation
            IPropagatorBlock<long, long>[] vertNodes = new IPropagatorBlock<long, long>[cntNodes];
            vertNodes[0] = new BufferBlock<long>();
            for (int i = 1; i < cntNodes; i++)
            {
                vertNodes[i] = new TransformBlock<long, long>(x => f()); // f = LongOperation()
            }

            var linkOpt = new DataflowLinkOptions() { PropagateCompletion = true };

            // connections
            for (int i = 1; i < cntNodes; i++)
            {
                vertNodes[i - 1].LinkTo(vertNodes[i], linkOpt);
            }

            sw.Stop();
            Console.WriteLine("Creation time: " + sw.Elapsed);

            return new Tuple<IPropagatorBlock<long, long>, IPropagatorBlock<long, long>>(vertNodes[0], vertNodes[vertNodes.Length - 1]);
        }

        public static long LongOperation()
        {
            return isPrime(89) ? 42 : 37;
        }

        public static long LongOperationSum(long[] x)
        {
            long res = 0;
            for (int i = 0; i < x.Length; i++)
            {
                res += x[i];
            }
            return res;

            //return x.Sum();
        }

        public static long LongOperationArray(long[] a)
        {
            long lastRes = 0;
            for (int i = 0; i < a.Length; i++)
            {
                lastRes += LongOperation();
            }
            return lastRes;
        }

        public static bool isPrime(long n)
        {
            int k = 2;
            while (k * k <= n && n % k != 0)
                k++;
            return n >= 2 && k * k > n;
        }

        private static IPropagatorBlock<long, long> Create3CrossNode(int batchSize, Func<long[], long> f)
        {
            // creation
            IPropagatorBlock<long, long[]> target = new BatchBlock<long>(batchSize, new GroupingDataflowBlockOptions() { Greedy = true });
            IPropagatorBlock<long[], long> middle = new TransformBlock<long[], long>(x => f(x)); // f =  LongOperationArray            
            IPropagatorBlock<long, long> source = new BroadcastBlock<long>(x => x);

            // connection
            target.LinkTo(middle);
            middle.LinkTo(source);

            // completion
            target.Completion.ContinueWith(completion =>
            {
                if (completion.IsFaulted)
                    ((IDataflowBlock)middle).Fault(completion.Exception);
                else
                    middle.Complete();
            });

            middle.Completion.ContinueWith(completion =>
            {
                if (completion.IsFaulted)
                    ((IDataflowBlock)source).Fault(completion.Exception);
                else
                    source.Complete();
            });

            return DataflowBlock.Encapsulate(target, source);
        }

        private static IPropagatorBlock<long, long> Create2CrossNode_1(Func<long> f)
        {
            var target = new TransformBlock<long, long>(x => f()); // f = LongOperation
            var source = new BroadcastBlock<long>(x => x);

            // linking
            target.LinkTo(source);

            // completion            
            target.Completion.ContinueWith(completion =>
            {
                if (completion.IsFaulted)
                    ((IDataflowBlock)source).Fault(completion.Exception);
                else
                    source.Complete();
            });

            return DataflowBlock.Encapsulate(target, source);
        }

        private static IPropagatorBlock<long, long> Create2CrossNode_2(Func<long[], long> f)
        {
            const int BATCHSIZE = 2;
            // creation
            var target = new BatchBlock<long>(BATCHSIZE, new GroupingDataflowBlockOptions() { Greedy = true });
            var source = new TransformBlock<long[], long>(x => f(x)); // f = LongOperationArray

            // linking
            target.LinkTo(source);

            // completion            
            target.Completion.ContinueWith(completion =>
            {
                if (completion.IsFaulted)
                    ((IDataflowBlock)source).Fault(completion.Exception);
                else
                    source.Complete();
            });

            return DataflowBlock.Encapsulate(target, source);
        }

        private static IPropagatorBlock<long, long> Create3AverageNode(Func<long[], long> f)
        {
            const int BATCH_SIZE = 3;
            // create
            var target = new BatchBlock<long>(BATCH_SIZE);
            var source = new TransformBlock<long[], long>(x => f(x)); // f = LongOperationArray

            // link
            target.LinkTo(source);

            // completion            
            target.Completion.ContinueWith(completion =>
            {
                if (completion.IsFaulted)
                    ((IDataflowBlock)source).Fault(completion.Exception);
                else
                    source.Complete();
            });

            return DataflowBlock.Encapsulate(target, source);
        }

        private static IPropagatorBlock<long, long> CreateSumSoFarNode(int batchSize, Func<long[], long> f)
        {
            // create
            var target = new BatchBlock<long>(batchSize);
            var source = new TransformBlock<long[], long>(x => f(x)); // f = LongOperationArray

            // link
            target.LinkTo(source);

            // completion            
            target.Completion.ContinueWith(completion =>
            {
                if (completion.IsFaulted)
                    ((IDataflowBlock)source).Fault(completion.Exception);
                else
                    source.Complete();
            });

            return DataflowBlock.Encapsulate(target, source);
        }

        private static double BenchMark(String msg, Func<long, long> f)
        {
            int n = 50;
            double dummy = 0.0, st = 0.0, sst = 0.0, pass = 0.0;

            int cnt = 0;
            Stopwatch sw = new Stopwatch();
            for (int i = 0; i < n; i++)
            {
                sw.Restart();
                long res = f(i); //f.Invoke(i);                                               
                dummy += res;
                sw.Stop();
                //Thread.Sleep(5);
                double time = sw.ElapsedMilliseconds;
                pass += time;

                if (pass < 100) continue;                                
                cnt++;
                
                //Console.WriteLine("time= {0}", time);
                Console.WriteLine("time = {0}, i = {1}, res = {2}, totalTime = {3}", time, i, res, pass);
                
                Console.WriteLine(time +", "+ st);
                st += time;
                sst += time * time;
            }

            double mean = st / (n - 15), sdev = Math.Sqrt((sst - mean * mean * (n - 15)) / (n - 15 - 1));
            //double mean = st / cnt, sdev = Math.Sqrt((sst - mean * mean * cnt) / (cnt-1));
            const string format = "{0:0.####}, Mean: {1:0.####} ms., Sdev: {2:0.####} ms.";
            //const string format = "{0} {1} {2}";
            string formatedStr = string.Format(format, msg, mean, sdev);
            Console.WriteLine(formatedStr);
            return dummy / n;
        }

        private static void Do(Func<int, int> f)
        {
            int n = 10; //, totalCount = 0;
            double dummy = 0.0, runningTime = 0.0, st = 0.0, sst = 0.0;
            // do { 
            //count *= 2;
            st = sst = 0.0;
            for (int j = 0; j < n; j++)
            {
                Stopwatch t = new Stopwatch();
                //for (int i = 0; i < count; i++)
                dummy += f.Invoke(j);
                runningTime = (double)t.ElapsedMilliseconds;
                double time = runningTime * 1e9;
                st += time;
                sst += time * time;

            }
            //} while (runningTime < 0.25 && count < Integer.MAX_VALUE/2);
            double mean = st / n, sdev = Math.Sqrt((sst - mean * mean * n) / (n - 1));
            //System.out.printf("%-25s %15.1f ns %10.2f %10d%n", msg, mean, sdev, count);
        }
    }
}
