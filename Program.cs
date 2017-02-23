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
    class Program
    {
        const int VALUE = 99;

        static void Main(string[] args)
        {
            var graph = GenerateVerticalGraph(100);
            ExecuteVerticalGraph(graph);
        }

        private static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> GenerateSumSoFarGraph(int depth)
        {
            Stopwatch sw = new Stopwatch();
            sw.Restart();

            // create
            var firstLayer = new BroadcastBlock<long>[depth];
            var secondLayer = new IPropagatorBlock<long, long>[depth];
            for (int i = 0; i < depth; i++)
            {
                firstLayer[i] = new BroadcastBlock<long>(null);
                secondLayer[i] = CreateSumSoFarNode(i + 1);
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

        private static void ExecuteSumSoFarGraph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> graph)
        {
            int cntTrials = 5;
            for (int i = 0; i < cntTrials; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Restart();

                // version 3
                int cntLayerNodes = graph.Item1.Length;
                var tasks1 = new List<Task>();
                for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
                {
                    int k = nodeId;
                    tasks1.Add(graph.Item1[k].SendAsync(1));
                }

                var tasks2 = new List<Task>();
                for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
                {
                    int k = nodeId;
                    tasks2.Add(graph.Item2[k].ReceiveAsync());
                }

                Task.WaitAll(tasks1.ToArray());
                Task.WaitAll(tasks2.ToArray());

                // version 2
                //int cntLayerNodes = graph.Item1.Length;
                //Parallel.For(0, cntLayerNodes, nodeId => { graph.Item1[nodeId].Post(1); });
                //Parallel.For(0, cntLayerNodes, nodeId => { graph.Item2[nodeId].Receive(); });

                // version 1
                //int cntLayerNodes = graph.Item1.Length;
                //for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
                //{
                //    graph.Item1[nodeId].Post(1);
                //}

                //for (int nodeId = 0; nodeId < cntLayerNodes; nodeId++)
                //{
                //    graph.Item2[nodeId].Receive();
                //}

                sw.Stop();
                Console.WriteLine("Execution: {0} - {1}", i + 1, sw.Elapsed);
            }
        }

        private static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> Generate3AverageGraph(int depth)
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
                secondLayer[i] = Create3AverageNode();
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

        private static void Execute3AverageGraph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> graph)
        {
            int depth = graph.Item1.Length;
            Random r = new Random();
            int cntAttempts = 5;
            for (int i = 0; i < cntAttempts; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Restart();

                for (int col = 0; col < depth; col++)
                {
                    graph.Item1[col].Post(r.Next());
                }

                // version 2
                //var tasks = new List<Task<long>>();
                //for (int col = 0; col < depth - 2; col++)
                //{
                //    tasks.Add(graph.Item2[col].ReceiveAsync());
                //}

                //Task.WaitAll(tasks.ToArray());

                // version 1
                for (int col = 0; col < depth - 2; col++)
                {
                    graph.Item2[col].Receive();
                }

                sw.Stop();
                Console.WriteLine("Execution time {0}: {1}", i + 1, sw.Elapsed);
            }
        }

        private static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> Generate2CrossGraph(int depth)
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
                    graph[row, 0] = new BroadcastBlock<long>(x => LongOperation()); // new TransformBlock<long, long>(x => LongOperation());
                    graph[row, 1] = Create2CrossNode();
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

        private static void ExecuteCross2Graph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long, long>[]> graph)
        {
            // ??? Posting by using Parallel.For()
            // ? Receive() or AsynchReceive()
            // exeuction is fast

            Random r = new Random();
            int cntAttempts = 5;
            for (int i = 0; i < cntAttempts; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Restart();

                graph.Item1[0].Post(1);
                graph.Item1[1].Post(1);

                graph.Item1[0].Receive(); // should finish first
                //graph.Item2[1].Receive(); // should take bit longer time

                sw.Stop();
                Console.WriteLine("Execution time {0}: {1}", i + 1, sw.Elapsed);
            }
        }

        private static Tuple<IEnumerable<IPropagatorBlock<long, long>>, IEnumerable<IPropagatorBlock<long, long>>> Generate3CrossGraph(int cntNodes)
        {
            Stopwatch sw = new Stopwatch();
            sw.Restart();

            // creation
            IPropagatorBlock<long, long>[,] nodes = new IPropagatorBlock<long, long>[cntNodes, cntNodes];
            for (int row = 0; row < cntNodes; row++)
            {
                if (row == 0) // first row - BroadCastBl
                {
                    for (int col = 0; col < cntNodes; col++)
                    {
                        nodes[row, col] = new BroadcastBlock<long>(x => LongOperation());
                    }
                }

                if (row > 0) // the rest rows - Custom Block
                {
                    for (int col = 0; col < cntNodes; col++)
                    {
                        if (col == 0 || col == cntNodes - 1)
                        {
                            nodes[row, col] = Create3CrossNode(2); // first, last column
                        }
                        else
                        {
                            nodes[row, col] = Create3CrossNode(3); // middle column
                        }
                    }
                }
            }

            // linking
            for (int row = 0; row < cntNodes - 1; row++)
            {
                for (int col = 0; col < cntNodes; col++)
                {
                    if (col == 0)
                    {
                        nodes[row, col].LinkTo(nodes[row + 1, col]);
                        nodes[row, col].LinkTo(nodes[row + 1, col + 1]);
                    }

                    if (col > 0 && col < cntNodes - 1)
                    {
                        nodes[row, col].LinkTo(nodes[row + 1, col - 1]);
                        nodes[row, col].LinkTo(nodes[row + 1, col]);
                        nodes[row, col].LinkTo(nodes[row + 1, col + 1]);
                    }

                    if (col == cntNodes - 1)
                    {
                        nodes[row, col].LinkTo(nodes[row + 1, col - 1]);
                        nodes[row, col].LinkTo(nodes[row + 1, col]);
                    }
                }
            }

            var firstLayer = new IPropagatorBlock<long, long>[cntNodes];
            for (int i = 0; i < cntNodes; i++)
            {
                firstLayer[i] = nodes[0, i];
            }

            var lastLayer = new IPropagatorBlock<long, long>[cntNodes];
            for (int i = 0; i < cntNodes; i++)
            {
                lastLayer[i] = nodes[cntNodes - 1, i];
            }

            sw.Stop();
            Console.WriteLine("Creation time: {0}", sw.Elapsed);
            // ??IEnumerable or [,]
            return new Tuple<IEnumerable<IPropagatorBlock<long, long>>, IEnumerable<IPropagatorBlock<long, long>>>(firstLayer, lastLayer);
        }

        private static void Execute3CrossGraph(Tuple<IEnumerable<IPropagatorBlock<long, long>>, IEnumerable<IPropagatorBlock<long, long>>> graph)
        {
            // ??? Posting by using Parallel.For()
            // ? Receive() or AsynchReceive()
            // exeuction is fast

            Random r = new Random();
            int cntNodes = graph.Item1.Count();
            int cntAttempts = 5;
            for (int i = 0; i < cntAttempts; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Restart();
                //Parallel.For(0, cntNodes, idx =>

                //var tasks = new List<Task<long>>();
                //foreach (var node in graph.Item2)
                //{
                //    tasks.Add(node.ReceiveAsync());
                //}

                foreach (var node in graph.Item1)
                {
                    node.Post(r.Next(1000));
                }

                foreach (var node in graph.Item2)
                {
                    node.Receive();
                }

                //Task.WaitAll(tasks.ToArray());

                sw.Stop();
                Console.WriteLine("Execution time {0}: {1}", i + 1, sw.Elapsed);
            }
        }

        private static void ExecuteSumTypedGraph1(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long[], long>> graph)
        {
            Random r = new Random();
            int cntNodes = graph.Item1.Length;
            int cntAttempts = 5;
            for (int i = 0; i < cntAttempts; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Restart();
                foreach (var idx in Enumerable.Range(0, cntNodes))
                {
                    graph.Item1[idx].Post(r.Next(1000));
                }

                var res = graph.Item2.Receive();
                sw.Stop();
                Console.WriteLine("Execution time {0}: {1}", i + 1, sw.Elapsed);
            }
        }

        private static void ExecuteSumTypedGraph(Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long[], long>> graph)
        {
            // Posting by using Parallel.For()
            Random r = new Random();
            int cntNodes = graph.Item1.Length;
            int cntAttempts = 5;
            for (int i = 0; i < cntAttempts; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Restart();
                //foreach (var idx in Enumerable.Range(0, cntNodes))
                Parallel.For(0, cntNodes, idx =>
                {
                    graph.Item1[idx].Post(r.Next(1000));
                });

                var res = graph.Item2.Receive();
                sw.Stop();
                Console.WriteLine("Execution time {0}: {1}", i + 1, sw.Elapsed);
            }
        }

        private static Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long[], long>> GenerateSumTypedGraph(int cntNodes) // !!! Func<int,int> f
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
            IPropagatorBlock<long[], long> transformer = new TransformBlock<long[], long>(x => LongOperation());  // x => x.ToList().Sum()

            // linking
            foreach (var item in nodes)
            {
                item.LinkTo(batcher);
            }
            batcher.LinkTo(transformer);
            Console.WriteLine("Creation time: {0}", sw.Elapsed);
            return new Tuple<IPropagatorBlock<long, long>[], IPropagatorBlock<long[], long>>(nodes, transformer);
        }

        private static void ExecuteVerticalGraph(Tuple<IPropagatorBlock<int, int>, IPropagatorBlock<int, int>> vertGraph)
        {
            // If I use ActionBlock - it starts another trhead and finishes at some point?
            // If I don't use ActionBlock - .Receive() - waits to go trough the mesh
            const int n = 5;
            Stopwatch sw = new Stopwatch();            
            for (int i = 0; i < n; i++)
            {                
                sw.Restart();
                vertGraph.Item1.Post(232);
                int res = vertGraph.Item2.Receive();
                sw.Stop();
                Console.WriteLine("Execution time {0}: {1}. Result = {2}", i + 1, sw.Elapsed, res);
            }
        }

        private static Tuple<IPropagatorBlock<int, int>, IPropagatorBlock<int, int>> GenerateVerticalGraph(int cntNodes)
        {
            /*
             *  cntNodes = 1_000_000 -> out of memory exception
             *  MaxDegreeOfParallelism = 4 - doesn't help. We put 1 item and check. No many items in the buffer
             */

            Stopwatch sw = new Stopwatch();
            sw.Start();
            // creation
            IPropagatorBlock<int, int>[] vertNodes = new IPropagatorBlock<int, int>[cntNodes];
            for (int i = 0; i < cntNodes; i++)
            {
                vertNodes[i] = new TransformBlock<int, int>(x => LongOperation());
            }

            var linkOpt = new DataflowLinkOptions() { PropagateCompletion = true };

            // connections
            for (int i = 1; i < cntNodes; i++)
            {
                vertNodes[i - 1].LinkTo(vertNodes[i], linkOpt);
            }

            sw.Stop();
            Console.WriteLine("Creation time: " + sw.Elapsed);

            return new Tuple<IPropagatorBlock<int, int>, IPropagatorBlock<int, int>>(vertNodes[0], vertNodes[vertNodes.Length - 1]);
        }

        private static Tuple<IPropagatorBlock<int, int>, ActionBlock<int>> GenerateColumnGraph1(int cntNodes)
        {
            /*
             *  cntNodes = 1_000_000 -> out of memory exception
             */

            Stopwatch sw = new Stopwatch();
            sw.Start();
            // creation
            IPropagatorBlock<int, int>[] vertNodes = new IPropagatorBlock<int, int>[cntNodes];
            for (int i = 0; i < cntNodes; i++)
            {
                vertNodes[i] = new TransformBlock<int, int>(x => x);
            }

            var linkOpt = new DataflowLinkOptions() { PropagateCompletion = true };

            // connections
            for (int i = 1; i < cntNodes; i++)
            {
                vertNodes[i - 1].LinkTo(vertNodes[i], linkOpt);
            }

            // action block -> printer
            int[] arr = new int[1];
            var printer = new ActionBlock<int>(x => Console.WriteLine(x));
            vertNodes[vertNodes.Length - 1].LinkTo(printer, linkOpt);
            sw.Stop();
            Console.WriteLine("Creation time: " + sw.Elapsed);

            // post
            //sw.Start();
            //vertNodes[0].Post(1);
            //vertNodes[0].Complete();
            //printer.Completion.Wait();
            //sw.Stop();
            //Console.WriteLine("Execution time: " + sw.Elapsed);

            return new Tuple<IPropagatorBlock<int, int>, ActionBlock<int>>(vertNodes[0], printer);
        }

        private static int LongOperation()
        {
            return isPrime(89) ? 42 : 37;
        }

        private static bool isPrime(int n)
        {
            int k = 2;
            while (k * k <= n && n % k != 0)
                k++;
            return n >= 2 && k * k > n;
        }

        private static IPropagatorBlock<long, long> Create3CrossNode(int batchSize)
        {
            // creation
            IPropagatorBlock<long, long[]> target = new BatchBlock<long>(batchSize, new GroupingDataflowBlockOptions() { Greedy = true });
            IPropagatorBlock<long[], long> middle = new TransformBlock<long[], long>(x => LongOperation()); // (x => LongOperation());
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

        private static void UseCustom3CrossNode()
        {
            // c
            var sb1 = new BroadcastBlock<long>(null);
            var custB = Create3CrossNode(3);
            var bb1 = new BufferBlock<long>();
            var bb2 = new BufferBlock<long>();
            var bb3 = new BufferBlock<long>();

            // l
            sb1.LinkTo(custB);
            sb1.LinkTo(custB);
            sb1.LinkTo(custB);
            custB.LinkTo(bb1);
            custB.LinkTo(bb2);
            custB.LinkTo(bb3);

            sb1.Post(1);
            Console.WriteLine(bb1.Receive());
            Console.WriteLine(bb2.Receive());
            Console.WriteLine(bb3.Receive());
        }

        private static IPropagatorBlock<long, long> Create2CrossNode()
        {
            const int batchSize = 2;
            // creation
            var target = new BatchBlock<long>(batchSize, new GroupingDataflowBlockOptions() { Greedy = true });
            var source = new TransformBlock<long[], long>(x => LongOperation());

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

        private static IPropagatorBlock<long, long> Create3AverageNode()
        {
            const int BATCH_SIZE = 3;
            // create
            var target = new BatchBlock<long>(BATCH_SIZE);
            var source = new TransformBlock<long[], long>(x => x.ToList().Sum());

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

        private static IPropagatorBlock<long, long> CreateSumSoFarNode(int batchSize)
        {
            // create
            var target = new BatchBlock<long>(batchSize);
            //var source = new TransformBlock<long[], long>(x => x.ToList().Sum());
            var source = new TransformBlock<long[], long>(x => LongOperation());

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

        private static double Mark7(String msg, Func<int, int> f)
        {
            int n = 10; //, count = 1, totalCount = 0;
            double dummy = 0.0, runningTime = 0.0, st = 0.0, sst = 0.0;
            //do { 
            //      count = 10;
            st = sst = 0.0;
            //for (int j=0; j<n; j++) {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < n; i++)
                //dummy++;
                dummy += f.Invoke(1);

            sw.Stop();
            //runningTime = t.check();
            runningTime = sw.ElapsedMilliseconds;
            double time = runningTime;
            st += time;
            sst += time * time;
            //totalCount += count;
            //}
            //} while (runningTime < 0.25 && count < Integer.MAX_VALUE/2);
            double mean = st / n, sdev = Math.Sqrt((sst - mean * mean * n) / (n - 1));
            Console.WriteLine("%-25s %15.1f ns %10.2f ", msg, mean, sdev);
            return dummy / n;
        }
    }
}
