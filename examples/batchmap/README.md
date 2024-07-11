## BatchMap Interface
The BatchMap interface allows developers to 
process multiple data items together in a single UDF handler.


### What is BatchMap?
BatchMap is an interface that allows developers to process multiple data items 
in a UDF single call, rather than each item in separate calls. 


The BatchMap interface can be helpful in scenarios 
where performing operations on a group of data can be more efficient.


### Understanding the User Interface
The BatchMap interface requires developers to implement a handler with a specific signature.
Here is the signature of the BatchMap handler:

```python
async def handler(datums: AsyncIterable[Datum]) -> BatchResponses:
```
The handler takes an iterable of `Datum` objects and returns
`BatchResponses`. 
The `BatchResponses` object is a list of the *same length* as the input
datums, with each item corresponding to the response for one request datum.

To clarify, let's say we have three data items:

```json lines
data_1 = {"name": "John", "age": 25}
data_2 = {"name": "Jane", "age": 30}
data_3 = {"name": "Bob", "age": 45}
```

These data items will be grouped together by numaflow and
passed to the handler as an iterable:

```python
result = await handler([data_1, data_2, data_3])
```

The result will be a BatchResponses object, which is a list of responses corresponding to each input data item's processing. 

### Important Considerations
When using BatchMap, there are a few important considerations to keep in mind:

- Ensure that the `BatchResponses` object is tagged with the *correct request ID*. 
Each Datum has a unique ID tag, which will be used by Numaflow to ensure correctness.

```python
async for datum in datums:
    batch_response = BatchResponse.new_batch_response(datum.id)
```


- Ensure that the length of the `BatchResponses`
list is equal to the number of requests received. 
**This means that for every input data item**, there should be a corresponding 
response in the BatchResponses list.

Use batch processing only when it makes sense. In some 
scenarios, batch processing may not be the most 
efficient approach, and processing data items one by one 
could be a better option.
The burden of concurrent processing of the data will rely on the 
UDF implementation in this use case.
