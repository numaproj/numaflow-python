# SideInput Example

An example that demonstrates how to write a `sideinput` function in python

### SideInput
```python
def my_handler() -> Response:
    time_now = datetime.datetime.now()
    # val is the value to be broadcasted
    val = "an example:" + str(time_now)
    global counter
    counter += 1
    # broadcast every other time
    if counter % 2 == 0:
        # no_broadcast_message() is used to indicate that there is no broadcast
        return Response.no_broadcast_message()
    # broadcast_message() is used to indicate that there is a broadcast
    return Response.broadcast_message(val.encode("utf-8"))
```
After performing the retrieval/update for the side input value the user can choose to either broadcast the 
message to other side input vertices or drop the message. The side input message is not retried.

For each side input there will be a file with the given path and after any update to the side input value the file will 
be updated.

The directory is fixed and can be accessed through sideinput constants `SideInput.SIDE_INPUT_DIR_PATH`.
The file name is the name of the side input.
```python
SideInput.SIDE_INPUT_DIR_PATH -> "/var/numaflow/side-inputs"
sideInputFileName -> "/var/numaflow/side-inputs/sideInputName"
```

### User Defined Function

The UDF vertex will watch for changes to this file and whenever there is a change it will read the file to obtain the new side input value.


### Pipeline spec

In the spec we need to define the side input vertex and the UDF vertex. The UDF vertex will have the side input vertex as a side input.

Side input spec:
```yaml
spec:
  sideInputs:
    - name: vertex
      udf:
        container:
          ....
      sideInputs:
        - myticker