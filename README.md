# Kafka Recipes

## Sorting events in Kstreams

Follow-up to my answer at https://stackoverflow.com/a/62722506 in the module [kstream-sort-shorter-duration](/kstream-sort-shorter-duration) 

* Buffer events for X seconds.
* After X seconds is elapsed, sort the events in buffer and send it downstream
* Use StateStore to buffer events - Resilience when the app crashes before the buffered events could be processed. Processor is at [SortProcessor](kstream-sort-shorter-duration/src/main/java/com/foo/SortProcessor.java)
* Buffer window cannot be set arbitrarily - transaction timeout and max poll interval should be set accordingly. More comments in the [code](kstream-sort-shorter-duration/src/main/java/com/foo/App.java) itself.
* Also demonstrated the use of `TopologyTestDriver` to test Processors

### Buffering events for longer duration

The X for buffer interval above is limited by the transaction timeout. 
What if you need to set a higher value to X? 
In that case, the processor should take care of drawing a buffer boundary within the state store and be able to determine when to close the current buffer and flush them, and simultaneously open a new one.

More to come...


## Deduplication 

Depending on Kafka Topic's retention policy, you can either delete or compact the closed segments. 
Note that compaction is only applicable for Kafka records with Key. 
There could be cases where you are producing without any keys. 
Your business would give you a KeySupplier based on which you could be asked to deduplicate messages.  
Also note that Kafka's compaction != deduplication.

If a topic has a live consumer consuming messages off the topic as and when they are produced, i.e. you are reading off the active segment, the records are not compacted for this real time consumer. 
This is why compaction is not the same as deduplication. If you have another consumer which is taping into this topic much later after the compaction has happened, this consumer will not see duplicate messgaes. 

But what you want, is consistent behavior - regardless of "when" you are reading the topic (before/after compaction shouldn't matter) 

Let's now talk when duplicates can happen :
- when the producer itself is producing duplicates - i.e. I have an application which is stuck replaying the same event. 
- when kafka client in your producer application has sent the intended message several times to the broker because of the network retries. 

The first scenario is an application error - not in control of Kafka. The second can be avoided by configuring idempotent Kafka producer.

The process of deduplication should happen exactly-once. i.e.
`Consume from input-topic -> determine if the message is duplicate -> Produce the non-duplicate into output-topic` 
The entire thing should work in all-or-none mode. Check out the properties needed for consumer and producer for this at [DedupKafkaUtil](dedup/src/main/java/com/foo/dedup/DedupKafkaUtil.java)

Is that it? Nope.
1. All components reading from the output-topic should be idempotent. i.e. the pipeline after this layer should be working in eos guarantee. 
If the next component after the output-topic is a sink, that should again, be idempotent. 
Also, the consumer of output-topic should be consuming messages with `read_committed` isolation. 
Check out [DedupRunnableTest](dedup/src/test/java/com/foo/dedup/DedupRunnableTest.java) to get complete picture - there is an upstream producer which is producing into input-topic and downstream consumer reading off from output-topic.  

1. Notice the middle step where you determine whether the message is duplicate or not? 
How can you determine what you are reading is a duplicate or not? Only by remembering everything you have read in the past. i.e. you need a store here. I have used RocksDB.
That's not it. This step needs to be resilient for failures. Note that this dedup-store is not included in the Kafka transaction. 
So if the transaction is failed, dedup-store should recover from the failure. 
It is quite easy to implement if you also store the kafka message's offset in the store and determine isDuplicate accordingly. Check out [DedupStore](dedup/src/main/java/com/foo/dedup/DedupStore.java) 
 
You would need to take backup of this dedup-store constantly. So if the original store got corrupted, you have to re-instantiate from the backup - and there is a slight chance that this backup is not up-to-date. How do you rebuild the state then?
Quite simple. You have a store between two persistent queues - so you can always check what is the offset consumed and the last message produced and rebuild the store!
 
This is a cool recipe I tried out with Kafka. But quite frankly, I have never found an use case for this recipe. Almost always, it is easier to have an idempotent sink than to go through this pain. Also, eos means you are using Kafka transaction - 
and that's always going to reduce the throughput since a transaction coordinator is involved behind the scenes.     


 