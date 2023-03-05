# 摘要

用户定义Map函数 处理key/value键值对 产生中间的key/value键值对

用户定义Reduce函数 合并有着相同中间key的value

系统自动并行在大规模集群上工作

运行时系统自动处理（切割输入数据，在机器间调度程序的执行，处理机器故障，管理机器间通信）

# 1 前言

Google处理大量raw data，用户计算产生导出数据。

问题：输入数据量很大，多台机器在一个可接受的时间内完成任务。实现大量复杂代码进行并行化计算、数据分发、处理故障

解决：设计一种抽象，将并行化、容错、数据分发、负载均衡等隐藏在库中。

# 2 编程模型

输入：一系列键值对

输出：一系列键值对

计算模型：

- Map：取一个键值对，并且产生一系列中间键值对。MapReduce库将相同中间键i的值聚合在一起，将他们传递给Reduce函数
- Reduce：接收一个中间键i和中间键i对应的一系列中间值。Reduce通过这些值合并来组成一个更小的集合。通常Reduce只产生0个或1个输出值（未读懂的点：Reduce 函数一般通过一个迭代器来获取中间值，从而在中间值的数目远远大于内存容量时，也能够处理。）

## 2.1 样例

统计文档中每个单词出现的次数，将输入文件和输出文件记录到MapReduceSpecification中

// 输出文件
// /gfs/test/freq-00000-of-00100
// /gfs/test/freq-00001-of-00100

```
　　map(String key, String value):
　　　　// key: document name
　　　　// value: document contents
　　　　for each word w in value:
　　　　　　EmitIntermediate(w, "1");
　　　　　　
　　reduce(String key, Iterator values):
　　　　// key: a word
　　　　// values: a list of counts
　　　　int result = 0;
　　　　for each v in values:
　　　　　　result += ParseInt(v);
　　　　Emit(AsString(result));
```

# 3 实现

以太网集群环境，自研文件系统管理集群上磁盘中的文件，通过Replication提供可用性和可靠性

用户提交jobs给调度系统，每个job由一系列的task组成，由调度器分配到集群中一系列可用的机器上

## 3.1 执行概述

通过将输入文件自动分割成M份，Map函数在多台机器上分布式执行。每一个输入都能并行地在不同的机器上执行。通过划分函数（例如，hash(key) mod R）将中间键划分为R份，Reduce函数也能被分布式地调用。其中划分的数目R和划分函数都是由用户指定的。

![image-20230210164942312](/Users/bytedance/Library/Application Support/typora-user-images/image-20230210164942312.png)

动作流程：

1. 用户程序中的MapReduce库首先将输入文件划分为(M)片，每片大小一般在16MB到64MB之间。之后在集群的多台机器上都启动了相同的程序拷贝。
2. 一个拷贝程序为master，剩下的都是worker，worker接收master分配的任务。其中有M个Map任务和R个Reduce任务要分配。master挑选一个空闲的worker并且给它分配一个map任务或者reduce任务。
3. 被分配到Map任务的worker读取输入块的内容。它从输入文件中解析出键值对并且将每个键值对传送给用户定义的Map函数。由Map函数产生的中间键值对缓存在内存中。
4. 被缓存的键值对会阶段性地写回本地磁盘，并且被划分函数分割成R份，这些缓存键值对在磁盘上的位置会回传给master，master将这些位置转发给Reduce worker。
5. Reduce worker从master接收到位置信息，使用RPC从Map worker的本地磁盘中获取缓存的数据。当Reduce worker读入全部的中间数据之后，会根据中间键对它们进行排序，使得所有具有相同键的键值对都聚集在一起。排序是必要的，因为许多不同的键被映射到同一个Reduce task中。如果中间数据的数量太大，以至于不能够装入内存，还需要另外的排序。
6. Reduce worker遍历已经排完序的中间数据，每当遇到一个新的中间键，会将key和相应的中间值传递给用户定义的Reduce函数。Reduce函数的输出会被添加到这个Reduce部分的输出文件中。
7. 当所有的Map tasks和Reduce tasks都已经完成的时候，master将唤醒用户程序。到此为止，用户代码中的MapReduce调用返回。

当成功执行完之后，MapReduce的执行结果被存放在R个输出文件中（每个Reduce task对应一个，文件名由用户指定）。通常用户并不需要将R个输出文件归并成1个。因为它们通常将这些文件作为另一个MapReduce调用的输入，或者将它们用于另外一个能够以多个文件作为输入的分布式应用。

## 3.2 Master数据结构

Master保存了

- 每个Map task和Reduce task的状态（idle，in-progress或者completed）
- 每个Map task和Reduce task的worker所在机器的标识（非idle空转状态的tasks）

Master相当于管道，Map task产生的中间文件通过Master传递给Reduce task。当Map task完成的时候，Master会收到位置和大小的更新信息，信息会逐渐被推送到处于in-progress状态的Reduce task中。

## 3.3 容错

### Worker故障

Master会周期性地ping每一个Worker。经过一个特定的时间还没有从Worker上获得响应，Master将Worker标记为failed。该Worker完成的Map task/正在执行的Map task/正在执行的Reduce task都回退为idle状态，因此可以重新调度到其他的Worker上。

已经完成的Map task需要重新执行是因为，输出保存在本地磁盘，发生故障就不能获取了。已经完成的Reduce task不需要重新执行，因为输出保存在全局的文件系统中。

Map task先由Worker A执行，后由Worker B执行。所有执行Reduce task的Worker都会收到重新执行的通知。未从Worker A中读取数据的Reduce task会从Worker B中读取数据。

### Master故障

对Master数据结构做周期性快照。一个Master task死了，可以根据最新快照重启一个Master task。

### 语义可能存在的故障

（一个确定性的程序，语义是程序的执行顺序。）

无论执行顺序、内部是否有误，当Map和Reduce都是相同输入得到相同输出的确定性函数，MapReduce函数获得的结果是确定的。

依赖于Map task和Reduce task原子性来实现上述特性。每一个in-progress Reduce task产生一个私有临时文件，每一个in-progress Map task产生R个私有临时文件。

当一个Map task完成时，Worker会给Master发送一个信息，其中包括R个文件名。如果完成信息是completed Map task完成，就忽略Map task。

当一个Reduce task完成时，Worker会将临时文件命名为最终输出文件，如果一个Reduce task在多台机器上运行，最终文件名可能会产生冲突，我们依赖文件系统的原子重命名操作来保证最终文件系统的数据来自一个Reduce task。

当Map和Reduce是非确定性函数，Reduce task可能读取不同的Map执行结果，Reduce task生成多个结果。

## 3.4 存储位置

网络带宽是计算资源中相对稀缺的资源，通过将输入数据存储在集群中每台机器的本地磁盘的方法来节省带宽，即GFS。MapReduce的Master获取所有输入文件的位置信息，然后将Map task调度到有相应输入文件副本的机器上。当发生故障时，将Map task调度到临近的具有该task输入文件副本的机器上。

## 3.5 任务粒度

Map操作分为M份，Reduce操作分为R份。理想情况下，M和R的值要比集群中worker机器数量更多。让一个worker同时进行许多不同的task有利于提高动态的负载均衡，同时在一个worker故障的时候能尽快恢复。

Master需要做O(M+R)个调度决定，还需要在内存中保存O(M*R)个状态，原因：对于每一个已经完成的Map task，Master会存储由它产生的R个中间文件的位置和大小。

R通常受限于用户，每个Reduce task的输出都分散在不同的输出文件中。可以选择M，使得每个输入文件大概16MB-64MB。让R为worker机器数量的较小倍数，M=20,0000，R=5000，worker机器=2000。

## 3.6 备用任务

水桶效应，MapReduce运行时间取决于运行速度最慢的worker，机器可能是CPU、内存、磁盘、网络带宽问题。

当MapReduce操作接近结束的时候，调用backup进程来处理in-progress任务，无论是原进程还是backup进程哪个先完成，master都将其标记为完成。

# 4 调优技巧

## 4.1 划分函数

用户决定Reduce task或者输出文件的数目R，通过一个划分函数，根据中间键值将各个task的数据进行划分，默认的划分是通过哈希函数，Hash(key) % R，用户自定义划分函数，希望同一个host的内容放在同一个输出文件中，Hash(Hostname(URLkey)) % R，来让所有来自于同一个host的URL的内容都输出到同一个输出文件。

## 4.2 顺序保证

在划分R中，中间键值都按键值的升序处理，输出文件通过key进行查询更方便。

## 4.3 Combiner

每个Map task会产生大量的中间键，例如word count中成千上万的<the, 1>。用户使用可选的Combiner函数，在网络传输之前进行归并操作。

Combiner和Reduce通常使用相同的代码，不同的是Reduce函数的输出写到最终输出文件中，Combiner函数的输出被写到一个Reduce task会读取的中间文件中

## 4.4 输入和输出

MapReduce库支持不同的格式的输入数据。比如文本模式，key是行数，value是该行内容。

程序员可以定义Reader接口来适应不同的输入类型。程序员需要保证必须能把输入数据切分成数据片段，且这些话宿儒片段能够由单独的Map任务来处理就行了。

Reader的数据源可能是数据库，可能是文本文件，甚至是内存等。输入Writer同样可以自定义。

## 4.5 副作用

一个task生成多个输出文件，但没有原子化多段commit操作。程序员自己保证生成的多个输出任务是确定性任务。

## 4.6 跳过损坏的记录

有时相比于修复不可执行的Bug，跳过该部分引起Bug的Record更加可取。因此，我们希望MapReduce检测到可能引起崩溃的Record时，自动跳过。

首先每个worker会通过一个handler来捕获异常，并利用一个全局变量来保存异常序号。worker会在之后发送给master的工作汇报中写上该signal序号（以UDP发送）。master看到该UDP包中存在多次故障，那么将来该worker失败了，master就不会重复执行该task，而是跳过该record。

## 4.7 本地执行

本地计算机模拟MapReduce任务的项目用于调试

## 4.8 状态信息

master内部有一个内置的HTTP服务器，可以用来展示一组状态信息页面。状态页面会显示计算进度，例如：已经完成的任务数量、正在执行的任务数量、输入的字节数、中间数据的字节数、输出的字节数、处理率等等。

这些页面也包含了指向每个任务的标准差以及生成的标准输出文件的链接。用户可以使用这些数据来预测计算需要多久才能完成，是否需要往该计算中增加更多资源。当计算消耗的时间比预期时间更长的时候，这些页面也可以用来找出为什么执行速度很慢的原因。

此外，顶层的状态页面会显示那些故障的worker，以及它们故障时正在运行的Map和Reduce任务。这些信息对于调试用户代码中的bug很有帮助。

## 4.9 计数器

MapReduce内部提供计数器机制，用来统计不同操作发生次数。要想使用计数器，程序员需要创建Counter对象，然后在Map和Reduce函数中以正确的方式增加counter。

当聚合这些counter的值时，master会去掉那些重复执行的相同map或者reduce操作的次数，以此避免重复计数（之前提到的备用任务和故障后重新执行任务，这两种情况会导致相同的任务被多次执行）。

有些counter值是由MapReduce库自动维护的，例如已经处理过的输入键值对的数量以及生成的输出键值对的数量。



