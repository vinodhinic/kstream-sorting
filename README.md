## Sorting events in Kstreams

###What

* Buffer events for X seconds
* After X seconds elapsed, sort the events in buffer and send it downstream
* Use StateStore to buffer events - Resilience when the app crashes before the buffered events could be processed. Processor is at [SortProcessor](/src/main/java/com/foo/SortProcessor.java)
* Buffer window cannot be set arbitrarily - transaction timeout and max poll interval should be set accordingly. More comments in the [code](/src/main/java/com/foo/App.java) itself.
* Also demonstrated the use of `TopologyTestDriver` to test Processors